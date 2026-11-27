package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"syscall"
)

const snapDeny = `# Case-insensitive shell patterns, matched against every path component.
*.pem
*.key
*.p12
*.pfx
*.ppk
id_*
.env*
.ssh
.aws
.kube
.docker
.gnupg
.netrc
_git-credentials
.git-credentials
.pypirc
credentials
credentials.*
credentials-*.json
*service-account*.json
*service_account*.json
client_secret*.json
kubeconfig
.credentials
.credentials.*
auth.json
token.json
tokens.json
access_token*
secrets.json
secrets.yml
secrets.yaml
application_default_credentials.json
.gitcookies
`

func snapName(name string) error {
	for c := range strings.SplitSeq(strings.ToLower(name), "/") {
		for p := range strings.SplitSeq(snapDeny, "\n") {
			if p != "" && !strings.HasPrefix(p, "#") {
				if ok, _ := path.Match(p, c); ok {
					return fmt.Errorf("sensitive path: %s", name)
				}
			}
		}
	}
	return nil
}
func validateUntrackedSnapshotFile(name string, raw []byte) error {
	if err := snapName(name); err != nil {
		return err
	}
	if snapSecret.Match(raw) || strings.EqualFold(path.Base(name), ".npmrc") && snapAuth.Match(raw) || bytes.HasPrefix(raw, []byte("version https://git-lfs.github.com/spec/v1\n")) {
		return fmt.Errorf("sensitive content or unresolved LFS: %s", name)
	}
	return nil
}

func snapList(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(s, "\x00"), "\x00")
}

// Expand only configured patterns, without following a symlink directory when
// selecting files. A directory match alone does not satisfy a pattern.
func snapIncludes(repo string, patterns []string) ([]string, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	root, e := os.OpenRoot(repo)
	if e != nil {
		return nil, e
	}
	defer closeBestEffort(root)
	files := map[string]bool{}
	var missing []string
	for _, pattern := range patterns {
		if !snapIncludePattern(pattern) {
			return nil, fmt.Errorf("invalid snapshot include pattern: %q", pattern)
		}
		names, e := fs.Glob(root.FS(), pattern)
		if e != nil {
			return nil, e
		}
		found := false
		for _, name := range names {
			if !snapIncludePath(name) {
				continue
			}
			st, e := root.Lstat(name)
			if os.IsNotExist(e) {
				continue
			}
			if e != nil {
				return nil, e
			}
			parent := filepath.Join(repo, filepath.FromSlash(path.Dir(name)))
			resolved, e := filepath.EvalSymlinks(parent)
			if e != nil {
				return nil, e
			}
			if st.IsDir() || resolved != parent {
				continue
			}
			files[name], found = true, true
		}
		if !found {
			missing = append(missing, fmt.Sprintf("%q", pattern))
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing snapshot include files: %s", strings.Join(missing, ", "))
	}
	var names []string
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	return names, nil
}

type nestedGitMetadata string

func (e nestedGitMetadata) Error() string {
	return fmt.Sprintf("nested Git metadata: %q", string(e))
}

func snapStage(source, stage snapGit, head, tmp string, isGit bool, include []string) (string, error) {
	modes := map[string]string{}
	tracked := map[string]bool{}
	indexed := map[string]string{}
	if isGit {
		entries, e := source.input(nil, "ls-files", "--stage", "-z")
		if e != nil {
			return "", e
		}
		for _, v := range snapList(string(entries)) {
			meta, n, _ := strings.Cut(v, "\t")
			f := strings.Fields(meta)
			if len(f) != 3 {
				return "", fmt.Errorf("%s: invalid Git index entry for %q", source.dir, n)
			}
			if f[2] != "0" {
				return "", fmt.Errorf("%s: unresolved merge in Git index for %q. Resolve the conflict and stage the file, run git merge --abort if merging, or dispatch from another worktree", source.dir, n)
			}
			if f[0] == "160000" {
				return "", fmt.Errorf("%s: snapshot does not support Git submodule %q in the index. Dispatch from another worktree without the submodule", source.dir, n)
			}
			modes[n] = f[0]
			indexed[n] = f[1]
		}
		tags, e := source.input(nil, "ls-files", "-t", "-z")
		if e != nil {
			return "", e
		}
		for _, t := range snapList(string(tags)) {
			if strings.HasPrefix(t, "S ") {
				return "", errors.New("sparse checkout unsupported")
			}
		}
		if head != "" {
			b, treeErr := source.input(nil, "ls-tree", "-rz", head)
			if treeErr != nil {
				return "", treeErr
			}
			for _, v := range snapList(string(b)) {
				meta, n, _ := strings.Cut(v, "\t")
				if strings.HasPrefix(meta, "160000 ") {
					return "", fmt.Errorf("%s: snapshot does not support Git submodule %q in HEAD. Dispatch from another worktree without the submodule", source.dir, n)
				}
				tracked[n] = true
			}
		}
		for i, args := range [][]string{{"ls-files", "-o", "--exclude-standard", "-z"}, {"ls-files", "-o", "-z", "--", "internal/web/dist"}, {"ls-files", "-oi", "--exclude-standard", "--directory", "-z"}} {
			b, listErr := source.input(nil, args...)
			if listErr != nil {
				return "", listErr
			}
			for _, n := range snapList(string(b)) {
				if listErr = snapName(n); listErr != nil {
					return "", listErr
				}
				if i < 2 {
					if _, ok := modes[n]; !ok {
						modes[n] = ""
					}
				}
			}
		}
		for _, n := range include {
			if _, ok := modes[n]; !ok {
				modes[n] = ""
			}
		}
		var names bytes.Buffer
		for n := range modes {
			names.WriteString(n)
			names.WriteByte(0)
		}
		attrs, e := source.input(names.Bytes(), "check-attr", "-z", "--stdin", "filter")
		if e != nil {
			return "", e
		}
		for i, v := range snapList(string(attrs)) {
			if i%3 == 2 && v != "unspecified" && v != "unset" {
				return "", errors.New("unsupported active clean filter")
			}
		}
	} else {
		e := filepath.WalkDir(source.dir, func(p string, d fs.DirEntry, e error) error {
			if e != nil {
				return e
			}
			n, relErr := filepath.Rel(source.dir, p)
			if relErr != nil {
				return relErr
			}
			if strings.EqualFold(d.Name(), ".git") {
				return nestedGitMetadata(filepath.ToSlash(n))
			}
			if !d.IsDir() {
				modes[filepath.ToSlash(n)] = ""
			}
			return nil
		})
		if e != nil {
			return "", e
		}
	}
	var records bytes.Buffer
	for n, mode := range modes {
		p := filepath.Join(source.dir, filepath.FromSlash(n))
		parent, e := filepath.EvalSymlinks(filepath.Dir(p))
		if e != nil {
			if os.IsNotExist(e) || errors.Is(e, syscall.ENOTDIR) {
				if _, required := slices.BinarySearch(include, n); required {
					return "", fmt.Errorf("snapshot include disappeared: %s", n)
				}
				continue
			}
			return "", e
		}
		if parent != filepath.Dir(p) {
			if _, required := slices.BinarySearch(include, n); required {
				return "", fmt.Errorf("snapshot include parent changed: %s", n)
			}
			continue
		}
		st, e := os.Lstat(p)
		if os.IsNotExist(e) || errors.Is(e, syscall.ENOTDIR) {
			if _, required := slices.BinarySearch(include, n); required {
				return "", fmt.Errorf("snapshot include disappeared: %s", n)
			}
			continue
		}
		if e != nil {
			return "", e
		}
		if st.IsDir() {
			if _, required := slices.BinarySearch(include, n); required {
				return "", fmt.Errorf("snapshot include became a directory: %s", n)
			}
			if mode == "" {
				return "", fmt.Errorf("nested repository: %s", n)
			}
			continue
		}
		var raw []byte
		switch {
		case st.Mode()&os.ModeSymlink != 0:
			t, linkErr := os.Readlink(p)
			if linkErr != nil {
				return "", linkErr
			}
			if !snapLinkInside(source.dir, parent, t) {
				return "", fmt.Errorf("nonportable symlink: %s", n)
			}
			mode = "120000"
			raw = []byte(t)
		case st.Mode().IsRegular():
			raw, e = os.ReadFile(p)
			if e != nil {
				return "", e
			}
			if st.Mode().Perm()&0100 != 0 || runtime.GOOS == "windows" && mode == "100755" {
				mode = "100755"
			} else {
				mode = "100644"
			}
		default:
			return "", fmt.Errorf("unsupported file: %s", n)
		}
		if !tracked[n] {
			if e = validateUntrackedSnapshotFile(n, raw); e != nil {
				return "", e
			}
		}
		oid := indexed[n]
		if oid == "" || !snapMatches(raw, oid) {
			hashed, hashErr := stage.input(raw, "hash-object", "-w", "--stdin")
			if hashErr != nil {
				return "", hashErr
			}
			oid = strings.TrimSpace(string(hashed))
		}
		records.WriteString(mode)
		records.WriteByte(' ')
		records.WriteString(oid)
		records.WriteByte('\t')
		records.WriteString(n)
		records.WriteByte(0)
	}
	stage.env = append(stage.env, "GIT_INDEX_FILE="+filepath.Join(tmp, "index"))
	if _, e := stage.input(records.Bytes(), "update-index", "-z", "--index-info"); e != nil {
		return "", e
	}
	return stage.get("write-tree")
}
func snapCapture(ctx context.Context, repo, store, tmp, history string, isGit bool, include []string) (SnapshotRepo, error) {
	r := SnapshotRepo{Include: include}
	g := snapGit{ctx: ctx, dir: repo}
	identity, format, head := repo, "sha1", ""
	var e error
	if isGit {
		identity, e = projectRepoIdentity(ctx, repo)
		if e != nil {
			return r, e
		}
		format, e = g.get("rev-parse", "--show-object-format")
		if e != nil {
			return r, e
		}
		head, e = g.get("rev-parse", "--verify", "-q", "HEAD")
		if e != nil {
			var x *exec.ExitError
			if !errors.As(e, &x) || x.ExitCode() != 1 {
				return r, e
			}
		}
		if head != "" {
			r.Head = &head
		}
	}
	r.RepoID = snapSum([]byte(identity))
	stage := snapGit{ctx: ctx, dir: filepath.Join(tmp, "stage.git"), env: []string{"GIT_AUTHOR_NAME=snapshot", "GIT_COMMITTER_NAME=snapshot", "GIT_AUTHOR_EMAIL=snapshot@invalid", "GIT_COMMITTER_EMAIL=snapshot@invalid", "GIT_AUTHOR_DATE=2000-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2000-01-01T00:00:00Z"}}
	if _, e = g.get("init", "-q", "--bare", "--object-format="+format, stage.dir); e != nil {
		return r, e
	}
	if isGit {
		obj, objectsErr := g.get("rev-parse", "--path-format=absolute", "--git-path", "objects")
		if objectsErr != nil {
			return r, objectsErr
		}
		if strings.ContainsAny(obj, "\r\n") {
			return r, errors.New("unsupported object directory")
		}
		if objectsErr = os.WriteFile(filepath.Join(stage.dir, "objects/info/alternates"), []byte(obj+"\n"), 0600); objectsErr != nil {
			return r, objectsErr
		}
	}
	tree, e := snapStage(g, stage, head, tmp, isGit, include)
	if e != nil {
		return r, e
	}
	if history != "" && (history != "HEAD" || isGit) {
		if head == "" {
			return r, errors.New("history requires committed HEAD")
		}
		r.History = true
		r.Base = head
		if history != "HEAD" {
			start, ok := strings.CutSuffix(history, "..HEAD")
			if !ok || start == "" || strings.Contains(start, "..") {
				return r, errors.New("history requires A..HEAD")
			}
			oid, historyErr := g.get("rev-parse", "--verify", "--end-of-options", start+"^{commit}")
			if historyErr != nil {
				return r, historyErr
			}
			if _, historyErr = g.get("merge-base", "--is-ancestor", oid, head); historyErr != nil {
				return r, historyErr
			}
			b, historyErr := g.get("rev-list", "--boundary", oid+".."+head)
			if historyErr != nil {
				return r, historyErr
			}
			for s := range strings.FieldsSeq(b) {
				if strings.HasPrefix(s, "-") {
					r.Shallow = append(r.Shallow, s[1:])
				}
			}
			if len(r.Shallow) == 0 {
				r.Shallow = []string{oid}
			}
			sort.Strings(r.Shallow)
			data, historyErr := snapJSON([]any{r.RepoID, r.Shallow})
			if historyErr != nil {
				return r, historyErr
			}
			r.StoreID = snapSum(data)
			if historyErr = snapImmutable(filepath.Join(stage.dir, "shallow"), []byte(strings.Join(r.Shallow, "\n")+"\n")); historyErr != nil {
				return r, historyErr
			}
		}
	} else if head != "" {
		ht, treeErr := g.get("rev-parse", head+"^{tree}")
		if treeErr != nil {
			return r, treeErr
		}
		r.Base, treeErr = stage.get("commit-tree", ht, "-m", "Source base v1")
		if treeErr != nil {
			return r, treeErr
		}
	}
	args := []string{"commit-tree", tree, "-m", "Source snapshot v1"}
	if r.Base != "" {
		args = append(args, "-p", r.Base)
	}
	r.OID, e = stage.get(args...)
	if e != nil {
		return r, e
	}
	bare := r.bare(store)
	if e = os.MkdirAll(filepath.Dir(bare), 0700); e != nil {
		return r, e
	}
	l, e := lockUntil(ctx, bare+".lock")
	if e != nil {
		return r, e
	}
	defer closeBestEffort(l)
	if e = initSnapshotBare(g, r, bare); e != nil {
		return r, e
	}
	if len(r.Shallow) > 0 {
		if e = snapImmutable(filepath.Join(bare, "shallow"), []byte(strings.Join(r.Shallow, "\n")+"\n")); e != nil {
			return r, e
		}
	}
	fetch := []string{"-c", "protocol.file.allow=always", "fetch", "--atomic", "--no-tags", "--no-auto-gc", "--no-write-fetch-head", stage.dir}
	for _, oid := range []string{r.OID, r.Base} {
		if oid != "" {
			ref := "refs/snapshots/" + oid
			if _, e = stage.get("update-ref", ref, oid); e != nil {
				return r, e
			}
			fetch = append(fetch, ref+":"+ref)
		}
	}
	if _, e = (snapGit{ctx: ctx, dir: bare}).get(fetch...); e != nil {
		return r, e
	}
	return r, syncSnapshotRepo(bare)
}
func Freeze(ctx context.Context, src string, opts FreezeOptions) (SnapshotID, Manifest, error) {
	var m Manifest
	store, e := snapStore()
	if e != nil {
		return "", m, e
	}
	src, e = filepath.Abs(src)
	if e != nil {
		return "", m, e
	}
	src, e = filepath.EvalSymlinks(src)
	if e != nil {
		return "", m, e
	}
	capture := func() ([]byte, error) {
		tmp, freezeErr := os.MkdirTemp("", "cx-freeze-")
		if freezeErr != nil {
			return nil, freezeErr
		}
		defer removeTreeBestEffort(tmp)
		layout, freezeErr := snapLayout(ctx, src, tmp, opts.Repositories)
		if freezeErr != nil {
			return nil, freezeErr
		}
		main, freezeErr := filepath.Rel(layout.root, layout.main)
		if freezeErr != nil {
			return nil, freezeErr
		}
		m = Manifest{Version: 1, Main: filepath.ToSlash(main), Workspace: layout.workspace}
		paths := make([]string, 0, len(layout.repos))
		for p := range layout.repos {
			paths = append(paths, p)
		}
		sort.Strings(paths)
		var includeErr error
		for _, p := range paths {
			names, err := snapIncludes(p, layout.repos[p])
			if err != nil {
				includeErr = errors.Join(includeErr, fmt.Errorf("%s: %w", p, err))
			}
			layout.repos[p] = names
		}
		if includeErr != nil {
			return nil, includeErr
		}
		for i, p := range paths {
			if snapWithin(p, store) {
				if !layout.git {
					return nil, errors.New("store inside source directory")
				}
				g := snapGit{ctx: ctx, dir: p}
				if _, err := g.get("check-ignore", "-q", "--", store); err != nil {
					return nil, errors.New("snapshot store must be ignored when inside a source repository")
				}
			}
			d := filepath.Join(tmp, fmt.Sprint(i))
			if freezeErr = os.Mkdir(d, 0700); freezeErr != nil {
				return nil, freezeErr
			}
			h := opts.History
			if h != "" && h != "HEAD" && !snapWithin(p, src) {
				h = "HEAD..HEAD"
			}
			r, repoErr := snapCapture(ctx, p, store, d, h, layout.git, layout.repos[p])
			if repoErr != nil {
				var nested nestedGitMetadata
				if len(opts.Repositories) > 0 && errors.As(repoErr, &nested) {
					repoErr = projectSnapshotSourceError(src, opts.Repositories, nested)
				}
				return nil, repoErr
			}
			rel, repoErr := filepath.Rel(layout.root, p)
			if repoErr != nil {
				return nil, repoErr
			}
			r.Path = filepath.ToSlash(rel)
			m.Repositories = append(m.Repositories, r)
		}
		if freezeErr = m.validate(); freezeErr != nil {
			return nil, freezeErr
		}
		return snapJSON(m)
	}
	a, e := capture()
	if e != nil {
		return "", m, e
	}
	b, e := capture()
	if e != nil {
		return "", m, e
	}
	if !bytes.Equal(a, b) {
		return "", m, errors.New("inputs changed while freezing")
	}
	id := snapSum(a)
	return SnapshotID(id), m, publishImmutable(filepath.Join(store, "manifests", id+".json"), a, filepath.Dir(store))
}

func projectSnapshotSourceError(src string, repos []projectRepository, nested nestedGitMetadata) error {
	containsMember := false
	members := make([]string, 0, len(repos))
	for _, repo := range repos {
		members = append(members, fmt.Sprintf("%q", repo.Path))
		if path, resolveErr := filepath.EvalSymlinks(repo.Path); resolveErr == nil && snapWithin(src, path) {
			containsMember = true
		}
	}
	if !containsMember {
		return nested
	}
	slices.Sort(members)
	return fmt.Errorf("DIR must be a project member repository. Project member paths: %s", strings.Join(members, ", "))
}

func snapMatches(raw []byte, oid string) bool {
	h := sha1.New()
	if len(oid) == 64 {
		h = sha256.New()
	}
	_, _ = fmt.Fprintf(h, "blob %d\x00", len(raw))
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil)) == oid
}
