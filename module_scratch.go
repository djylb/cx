package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

const defaultModuleOutputBytes int64 = 1 << 20

type budgetOutput struct {
	dst       io.Writer
	left      int64
	truncated bool
}

func (w *budgetOutput) Write(p []byte) (int, error) {
	n := min(int64(len(p)), w.left)
	w.truncated = w.truncated || n != int64(len(p))
	if n != 0 {
		if err := writeFrame(w.dst.Write, p[:n]); err != nil {
			return 0, err
		}
		w.left -= n
	}
	return len(p), nil
}

func moduleOutputBudget(n int64) int64 {
	if n > 0 {
		return n
	}
	return defaultModuleOutputBytes
}

func snapshotHookEnv(scratch string) []string {
	var env []string
	for _, key := range []string{"PATH", "SYSTEMROOT", "COMSPEC", "PATHEXT"} {
		if value, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+value)
		}
	}
	return append(env, "HOME="+scratch, "USERPROFILE="+scratch, "TMPDIR="+scratch, "TMP="+scratch, "TEMP="+scratch, "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0")
}

// A private checkout owns its index, refs and newly written objects. Existing
// objects can be read through Git's alternate object store.
func cloneModuleRepo(ctx context.Context, source, dest, oid string) error {
	parent := filepath.Dir(dest)
	if err := os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	g := snapGit{ctx: ctx, dir: parent}
	if _, err := g.get("clone", "-q", "--no-checkout", "--shared", "--", source, dest); err != nil {
		return err
	}
	if oid == "" {
		return nil
	}
	g.dir = dest
	if err := snapImmutable(filepath.Join(dest, ".git", "info", "attributes"), []byte(snapshotCheckoutAttributes)); err != nil {
		return err
	}
	if _, err := g.get("checkout", "-q", "--detach", oid); err != nil {
		return err
	}
	if runtime.GOOS == "windows" {
		return snapWindowsLinks(g)
	}
	return nil
}

type moduleScratch struct {
	dir, cwd string
	env      []string
}

// prepareSnapshotInput gives hooks private refs and a standalone checkout of
// the existing snapshot. The snapshot store remains authoritative for objects.
func prepareSnapshotInput(ctx context.Context, dir string, m Manifest) (moduleScratch, error) {
	out := moduleScratch{dir: dir, cwd: filepath.Join(dir, "tree", m.Main), env: snapshotHookEnv(dir)}
	store, err := snapStore()
	if err != nil {
		return out, err
	}
	tree := filepath.Join(dir, "tree")
	for _, repo := range m.Repositories {
		if err := cloneModuleRepo(ctx, repo.bare(store), filepath.Join(tree, repo.Path), repo.OID); err != nil {
			return out, err
		}
	}
	if m.Workspace == nil {
		out.env = append(out.env, "GOWORK=off")
		return out, nil
	}
	name := filepath.Join(dir, "go.work")
	// Workspace paths are relative to the directory containing go.work.
	if err := writeJobBytes(name, []byte(m.Workspace.Text)); err != nil {
		return out, err
	}
	if m.Workspace.Sum != nil {
		if err := writeJobBytes(name+".sum", []byte(*m.Workspace.Sum)); err != nil {
			return out, err
		}
	}
	out.env = append(out.env, "GOWORK="+name)
	return out, nil
}

func prepareAuxiliaryInput(ctx context.Context, job string, snapshot SnapshotID, result string, meta *turnResult) (out moduleScratch, err error) {
	if snapshot == "" {
		return out, errors.New("auxiliary input requires a sealed snapshot baseline")
	}
	manifest, err := loadSnapshot(snapshot)
	if err != nil {
		return out, err
	}
	tmp := filepath.Join(job, jobTempDir)
	if err = ensureCacheDir(tmp); err != nil {
		return out, err
	}
	dir, err := os.MkdirTemp(tmp, "cx-aux-")
	if err != nil {
		return out, err
	}
	defer func() {
		if err != nil {
			removeTreeBestEffort(dir)
		}
	}()
	out, err = prepareSnapshotInput(ctx, dir, manifest)
	if err != nil {
		return out, err
	}
	tree := filepath.Join(dir, "tree")
	if meta != nil {
		for _, patch := range meta.Repositories {
			if !snapPath(patch.Patch) || !slices.ContainsFunc(manifest.Repositories, func(repo SnapshotRepo) bool {
				return repo.Path == patch.Path && repo.RepoID == patch.RepoID && repo.OID == patch.SnapshotOID
			}) {
				return out, errors.New("acceptance patch does not match its snapshot")
			}
			g := snapGit{ctx: ctx, dir: filepath.Join(tree, patch.Path)}
			if _, err := g.get("apply", "--binary", "--whitespace=nowarn", "--", filepath.Join(result, patch.Patch)); err != nil {
				return out, err
			}
		}
	}
	if meta != nil {
		for _, attachment := range meta.Artifacts {
			if !snapPath(attachment.Path) || !strings.HasPrefix(attachment.Path, "artifacts/") {
				return out, errors.New("invalid acceptance attachment path")
			}
			target := filepath.Join(out.cwd, attachment.Path)
			if err := prepareArtifactTarget(out.cwd, target); err != nil {
				return out, err
			}
			if err := copyAcceptanceArtifact(ctx, filepath.Join(result, attachment.Path), target, attachment.Bytes); err != nil {
				return out, err
			}
		}
	}
	// Links are checked on the combined layout, after attachments are in place.
	if err := verifyAuxiliaryLinks(tree); err != nil {
		return out, err
	}
	return out, nil
}

// prepareArtifactTarget lets a sealed attachment replace a tracked regular
// file of the same name, after checking that no ancestor leaves the checkout.
func prepareArtifactTarget(root, target string) error {
	rel, err := filepath.Rel(root, filepath.Dir(target))
	if err != nil {
		return err
	}
	dir := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "." {
			continue
		}
		dir = filepath.Join(dir, part)
		info, statErr := os.Lstat(dir)
		if os.IsNotExist(statErr) {
			if err = os.Mkdir(dir, 0700); err != nil {
				return err
			}
			continue
		}
		if statErr != nil {
			return statErr
		}
		if !info.IsDir() {
			return errors.New("acceptance attachment ancestor is not a directory")
		}
	}
	info, err := os.Lstat(target)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("acceptance attachment target is not a regular file")
	}
	return os.Remove(target)
}

func copyAcceptanceArtifact(ctx context.Context, source, target string, expected int64) error {
	in, err := openJobFile(source, jobFileRead)
	if err != nil {
		return err
	}
	defer closeBestEffort(in)
	out, err := openJobFile(target, jobFileNew)
	if err != nil {
		return err
	}
	n, err := io.Copy(out, resultReader{ctx, in})
	if err = errors.Join(err, out.Close()); err != nil {
		return err
	}
	if n != expected {
		return errors.New("sealed acceptance attachment changed length")
	}
	return nil
}

func verifyAuxiliaryLinks(tree string) error {
	root, err := filepath.EvalSymlinks(tree)
	if err != nil {
		return err
	}
	return filepath.WalkDir(tree, func(p string, entry fs.DirEntry, err error) error {
		if err != nil || entry.Type()&os.ModeSymlink == 0 {
			return err
		}
		target, err := filepath.EvalSymlinks(p)
		if err != nil || !snapWithin(root, target) {
			return fmt.Errorf("auxiliary input symlink escapes its checkout or cannot be resolved: %s", p)
		}
		return nil
	})
}
