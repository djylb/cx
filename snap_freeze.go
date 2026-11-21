package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
)

//go:embed snap_denylist
var snapDeny string

func snapName(name string) error {
	for _, c := range strings.Split(strings.ToLower(name), "/") {
		for _, p := range strings.Split(snapDeny, "\n") {
			if p != "" && !strings.HasPrefix(p, "#") {
				if ok, _ := path.Match(p, c); ok {
					return fmt.Errorf("sensitive path: %s", name)
				}
			}
		}
	}
	return nil
}
func snapList(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(s, "\x00"), "\x00")
}
func snapStage(source, stage snapGit, head string, isGit bool, tmp string) (string, error) {
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
			if len(f) != 3 || f[2] != "0" || f[0] == "160000" {
				return "", errors.New("unsupported submodule or unresolved index")
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
			b, e := source.input(nil, "ls-tree", "-rz", head)
			if e != nil {
				return "", e
			}
			for _, v := range snapList(string(b)) {
				meta, n, _ := strings.Cut(v, "\t")
				if strings.HasPrefix(meta, "160000 ") {
					return "", errors.New("submodule in source HEAD")
				}
				tracked[n] = true
			}
		}
		for i, args := range [][]string{{"ls-files", "-o", "--exclude-standard", "-z"}, {"ls-files", "-o", "-z", "--", "internal/web/dist"}, {"ls-files", "-oi", "--exclude-standard", "--directory", "-z"}} {
			b, e := source.input(nil, args...)
			if e != nil {
				return "", e
			}
			for _, n := range snapList(string(b)) {
				if e = snapName(n); e != nil {
					return "", e
				}
				if i < 2 {
					if _, ok := modes[n]; !ok {
						modes[n] = ""
					}
				}
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
			if strings.EqualFold(d.Name(), ".git") {
				return errors.New("nested Git metadata")
			}
			if !d.IsDir() {
				n, e := filepath.Rel(source.dir, p)
				if e != nil {
					return e
				}
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
				continue
			}
			return "", e
		}
		if parent != filepath.Dir(p) {
			continue
		}
		st, e := os.Lstat(p)
		if os.IsNotExist(e) || errors.Is(e, syscall.ENOTDIR) {
			continue
		}
		if e != nil {
			return "", e
		}
		if st.IsDir() {
			if mode == "" {
				return "", fmt.Errorf("nested repository: %s", n)
			}
			continue
		}
		var raw []byte
		switch {
		case st.Mode()&os.ModeSymlink != 0:
			t, e := os.Readlink(p)
			if e != nil {
				return "", e
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
			if e := snapName(n); e != nil {
				return "", e
			}
			if snapSecret.Match(raw) || strings.EqualFold(path.Base(n), ".npmrc") && snapAuth.Match(raw) || bytes.HasPrefix(raw, []byte("version https://git-lfs.github.com/spec/v1\n")) {
				return "", fmt.Errorf("sensitive content or unresolved LFS: %s", n)
			}
		}
		if oid := indexed[n]; oid != "" && snapMatches(raw, oid) {
			fmt.Fprintf(&records, "%s %s\t%s\x00", mode, oid, n)
			continue
		}
		oid, e := stage.input(raw, "hash-object", "-w", "--stdin")
		if e != nil {
			return "", e
		}
		fmt.Fprintf(&records, "%s %s\t%s\x00", mode, bytes.TrimSpace(oid), n)
	}
	stage.env = append(stage.env, "GIT_INDEX_FILE="+filepath.Join(tmp, "index"))
	if _, e := stage.input(records.Bytes(), "update-index", "-z", "--index-info"); e != nil {
		return "", e
	}
	return stage.get("write-tree")
}
func snapCapture(ctx context.Context, repo, store, tmp, history string, isGit bool) (SnapshotRepo, error) {
	var r SnapshotRepo
	g := snapGit{ctx: ctx, dir: repo}
	identity, format, head := repo, "sha1", ""
	var e error
	if isGit {
		identity, e = g.get("rev-parse", "--path-format=absolute", "--git-common-dir")
		if e != nil {
			return r, e
		}
		identity, e = filepath.EvalSymlinks(identity)
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
		obj, e := g.get("rev-parse", "--path-format=absolute", "--git-path", "objects")
		if e != nil {
			return r, e
		}
		if strings.ContainsAny(obj, "\r\n") {
			return r, errors.New("unsupported object directory")
		}
		if e = os.WriteFile(filepath.Join(stage.dir, "objects/info/alternates"), []byte(obj+"\n"), 0600); e != nil {
			return r, e
		}
	}
	tree, e := snapStage(g, stage, head, isGit, tmp)
	if e != nil {
		return r, e
	}
	if history != "" && !(history == "HEAD" && !isGit) {
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
			oid, e := g.get("rev-parse", "--verify", "--end-of-options", start+"^{commit}")
			if e != nil {
				return r, e
			}
			if _, e = g.get("merge-base", "--is-ancestor", oid, head); e != nil {
				return r, e
			}
			b, e := g.get("rev-list", "--boundary", oid+".."+head)
			if e != nil {
				return r, e
			}
			for _, s := range strings.Fields(b) {
				if strings.HasPrefix(s, "-") {
					r.Shallow = append(r.Shallow, s[1:])
				}
			}
			if len(r.Shallow) == 0 {
				r.Shallow = []string{oid}
			}
			sort.Strings(r.Shallow)
			data, e := snapJSON([]any{r.RepoID, r.Shallow})
			if e != nil {
				return r, e
			}
			r.StoreID = snapSum(data)
			if e = snapImmutable(filepath.Join(stage.dir, "shallow"), []byte(strings.Join(r.Shallow, "\n")+"\n")); e != nil {
				return r, e
			}
		}
	} else if head != "" {
		ht, e := g.get("rev-parse", head+"^{tree}")
		if e != nil {
			return r, e
		}
		r.Base, e = stage.get("commit-tree", ht, "-m", "Source base v1")
		if e != nil {
			return r, e
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
	defer l.Close()
	if _, e = os.Stat(bare); os.IsNotExist(e) {
		if _, e = g.get("init", "-q", "--bare", "--object-format="+format, bare); e != nil {
			return r, e
		}
	} else if e != nil {
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
	_, e = (snapGit{ctx: ctx, dir: bare}).get(fetch...)
	return r, e
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
		tmp, e := os.MkdirTemp("", "cx-freeze-")
		if e != nil {
			return nil, e
		}
		defer os.RemoveAll(tmp)
		repos, root, w, isGit, e := snapLayout(ctx, src, tmp)
		if e != nil {
			return nil, e
		}
		main, e := filepath.Rel(root, src)
		if e != nil {
			return nil, e
		}
		m = Manifest{Version: 1, Main: filepath.ToSlash(main), Workspace: w}
		for i, p := range repos {
			if snapWithin(p, store) {
				if !isGit {
					return nil, errors.New("store inside source directory")
				}
				g := snapGit{ctx: ctx, dir: p}
				if _, err := g.get("check-ignore", "-q", "--", store); err != nil {
					return nil, errors.New("snapshot store must be ignored when inside a source repository")
				}
			}
			d := filepath.Join(tmp, fmt.Sprint(i))
			if e = os.Mkdir(d, 0700); e != nil {
				return nil, e
			}
			h := opts.History
			if h != "" && h != "HEAD" && !snapWithin(p, src) {
				h = "HEAD..HEAD"
			}
			r, e := snapCapture(ctx, p, store, d, h, isGit)
			if e != nil {
				return nil, e
			}
			rel, e := filepath.Rel(root, p)
			if e != nil {
				return nil, e
			}
			r.Path = filepath.ToSlash(rel)
			m.Repositories = append(m.Repositories, r)
		}
		if e = m.validate(); e != nil {
			return nil, e
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
	return SnapshotID(id), m, snapImmutable(filepath.Join(store, "manifests", id+".json"), a)
}

func snapMatches(raw []byte, oid string) bool {
	var h hash.Hash = sha1.New()
	if len(oid) == 64 {
		h = sha256.New()
	}
	fmt.Fprintf(h, "blob %d\x00", len(raw))
	h.Write(raw)
	return hex.EncodeToString(h.Sum(nil)) == oid
}
