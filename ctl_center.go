package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"go/build"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"

	"sort"
	"strconv"
	"strings"
	"time"
)

// Export the standalone build inputs so freezing cxrun never snapshots the
// surrounding deployment repository. Keep a stable source path for B2's
// per-repository incremental publication receipts.
func freezeCompilerSource(ctx context.Context) (SnapshotID, Manifest, error) {
	var empty Manifest
	module, e := moduleDir()
	if e != nil {
		return "", empty, e
	}
	home, e := os.UserHomeDir()
	if e != nil {
		return "", empty, e
	}
	dir := filepath.Join(home, ".cache", "cx", "compiler-source")
	if e = os.MkdirAll(dir, 0700); e != nil {
		return "", empty, e
	}
	lock, e := lockUntil(ctx, filepath.Join(dir, "export.lock"))
	if e != nil {
		return "", empty, e
	}
	defer closeBestEffort(lock)
	tmp, e := os.MkdirTemp(dir, ".export-")
	if e != nil {
		return "", empty, e
	}
	defer removeTreeBestEffort(tmp)
	files := map[string]bool{"go.mod": true, "go.sum": true}
	e = filepath.WalkDir(module, func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			return nil
		}
		if path != module && (strings.HasPrefix(d.Name(), ".") || d.Name() == "testdata") {
			return filepath.SkipDir
		}
		rel, e := filepath.Rel(module, path)
		if e != nil {
			return e
		}
		entries, e := os.ReadDir(path)
		if e != nil {
			return e
		}
		hasGo := false
		for _, entry := range entries {
			n := entry.Name()
			if entry.IsDir() || strings.HasSuffix(n, "_test.go") {
				continue
			}
			switch filepath.Ext(n) {
			case ".go":
				hasGo = true
			case ".s", ".h", ".syso":
			default:
				continue
			}
			files[filepath.Join(rel, n)] = true
		}
		if !hasGo {
			return nil
		}
		for _, platform := range []string{"linux", "windows", "darwin"} {
			bc := build.Default
			bc.GOOS = platform
			bc.CgoEnabled = false
			pkg, err := bc.ImportDir(path, build.ImportComment)
			var noGo *build.NoGoError
			if errors.As(err, &noGo) {
				continue
			}
			if err != nil {
				return err
			}
			for _, pattern := range pkg.EmbedPatterns {
				matches, e := filepath.Glob(filepath.Join(path, strings.TrimPrefix(pattern, "all:")))
				if e != nil {
					return e
				}
				for _, match := range matches {
					e = filepath.WalkDir(match, func(p string, d fs.DirEntry, e error) error {
						if e != nil || d.IsDir() {
							return e
						}
						n, e := filepath.Rel(module, p)
						if e == nil {
							files[n] = true
						}
						return e
					})
					if e != nil {
						return e
					}
				}
			}
		}
		return nil
	})
	if e != nil {
		return "", empty, e
	}
	var names []string
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	fingerprint := sha256.New()
	for _, n := range names {
		src := filepath.Join(module, n)
		st, copyErr := os.Lstat(src)
		if copyErr != nil {
			return "", empty, copyErr
		}
		if !st.Mode().IsRegular() {
			return "", empty, fmt.Errorf("invalid compiler input %s", n)
		}
		dst := filepath.Join(tmp, n)
		if copyErr = os.MkdirAll(filepath.Dir(dst), 0700); copyErr != nil {
			return "", empty, copyErr
		}
		data, copyErr := os.ReadFile(src)
		if copyErr != nil {
			return "", empty, copyErr
		}
		_, _ = fmt.Fprintf(fingerprint, "%s\x00%x\n", filepath.ToSlash(n), sha256.Sum256(data))
		if copyErr = os.WriteFile(dst, data, 0600); copyErr != nil {
			return "", empty, copyErr
		}
	}
	key := fmt.Sprintf("%x", fingerprint.Sum(nil))
	receipt := filepath.Join(dir, "last.json")
	var previous struct {
		Key string
		ID  SnapshotID
	}
	if readJSON(receipt, &previous) == nil && previous.Key == key {
		if m, loadErr := loadSnapshot(previous.ID); loadErr == nil {
			return previous.ID, m, nil
		}
	}
	target := filepath.Join(dir, "cxrun")
	if e = os.RemoveAll(target); e != nil {
		return "", empty, e
	}
	if e = os.Rename(tmp, target); e != nil {
		return "", empty, e
	}
	sid, m, e := Freeze(ctx, target, FreezeOptions{})
	if e == nil {
		previous.Key = key
		previous.ID = sid
		e = writeJSON(receipt, previous)
	}
	return sid, m, e
}
func compileSnapshot(ctx context.Context, sid, plat string) (binary string, err error) {
	path, e := cachePath(sid, plat)
	if e != nil {
		return "", e
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return "", e
	}
	l, e := transferLock(ctx, path+".lock")
	if e != nil {
		return "", e
	}
	defer closeBestEffort(l)
	if st, statErr := os.Stat(path); statErr == nil && st.Mode().IsRegular() {
		return path, nil
	}
	m, e := loadSnapshot(SnapshotID(sid))
	if e != nil {
		return "", e
	}
	tmp, e := os.MkdirTemp(filepath.Dir(path), ".compile-")
	if e != nil {
		return "", e
	}
	defer removeTreeBestEffort(tmp)
	prep, e := Materialize(ctx, tmp, m, "")
	if e != nil {
		return "", e
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		err = errors.Join(err, releaseSnapshotWorktrees(cleanup, tmp))
	}()
	target := filepath.Join(tmp, "binary")
	goos, arch, _ := strings.Cut(plat, "/")
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-buildvcs=false", "-o", target, ".")
	cmd.Dir = prep.Source
	cmd.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0", "GOOS="+goos, "GOARCH="+arch)
	cmd.Stderr = os.Stderr
	if e = cmd.Run(); e != nil {
		return "", e
	}
	return path, os.Rename(target, path)
}

func centerInstall(ctx context.Context, sid, plat, destination string) error {
	var dest endpoint
	if e := json.Unmarshal([]byte(destination), &dest); e != nil {
		return e
	}
	binary, e := compileSnapshot(ctx, sid, plat)
	if e != nil {
		return e
	}
	if e = setupArtifact(ctx, dest, binary); e != nil {
		return e
	}
	digest, e := fileHash(binary)
	if e == nil {
		fmt.Println(digest)
	}
	return e
}
func bootstrapCompiler(ctx context.Context, center endpoint, m Manifest) error {
	if len(m.Repositories) != 1 {
		return errors.New("expected standalone compiler snapshot")
	}
	store, e := snapStore()
	if e != nil {
		return e
	}
	row := m.Repositories[0]
	g := snapGit{ctx: ctx, dir: row.bare(store)}
	archive, e := g.input(nil, "archive", "--format=tar.gz", row.OID)
	if e != nil {
		return e
	}
	// On a fresh Linux center the receiver itself does not exist yet. Build
	// that first native receiver from a source-only archive sent via stdin.
	cmdline := "set -eu; umask 077; d=$(mktemp -d); trap 'rm -rf \"$d\"' EXIT; tar -xzf - -C \"$d\"; (cd \"$d\" && GOWORK=off CGO_ENABLED=0 go build -trimpath -buildvcs=false -o cx .); "
	if center.State != "" {
		cmdline += "export CXRUN_STATE=" + shellQuote(center.State) + "; "
	}
	cmdline += "sum=$(\"$d/cx\" node version); \"$d/cx\" node install \"$d/cx\" " + shellQuote(center.CX) + " \"$sum\""
	opts, e := sshOptions()
	if e != nil {
		return e
	}
	cmd := exec.CommandContext(ctx, "ssh", append(opts, "--", center.SSH, cmdline)...)
	cmd.Stdin = bytes.NewReader(archive)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// A relay owns its LAN route to the target. A direct Linux endpoint builds
// for itself. Other direct endpoints retain their configured transport.
func deployFromCenter(ctx context.Context, h endpoint, plat string) (handled bool, err error) {
	if os.Getenv("CX_SETUP_BINARY") != "" || h.SSH == "" {
		return false, nil
	}
	center := h
	target := h
	target.relay = nil
	target.Via = ""
	if h.relay != nil {
		center = *h.relay
	} else {
		target.SSH = ""
	}
	if !strings.HasPrefix(center.Platform, "linux/") || center.relay != nil {
		return false, nil
	}
	_, e := center.rawSSH(ctx, "ssh", []string{"--", center.SSH, "command -v go >/dev/null || exit 42"}, "", 10*time.Second)
	if e != nil {
		var ex *exec.ExitError
		if errors.As(e, &ex) && ex.ExitCode() == 42 {
			return false, nil
		}
		return true, e
	}
	sid, m, e := freezeCompilerSource(ctx)
	if e != nil {
		return true, e
	}
	capability, e := center.call(ctx, 10*time.Second, "compiler-protocol")
	if e != nil {
		return true, e
	}
	if capability.code != 0 || strings.TrimSpace(string(capability.data)) != "1" {
		if e = bootstrapCompiler(ctx, center, m); e != nil {
			return true, e
		}
	}
	if e = Publish(ctx, center, sid, nil); e != nil {
		return true, e
	}
	b, e := json.Marshal(target)
	if e != nil {
		return true, e
	}
	r, e := center.invoke(ctx, 6*time.Minute, false, "compiler-install", string(sid), plat, string(b))
	if e != nil {
		return true, e
	}
	if r.code != 0 {
		return true, r.output()
	}
	if digest := strings.TrimSpace(string(r.data)); snapHex(digest, 64) {
		exe, e := os.Executable()
		if e != nil {
			return true, e
		}
		version, e := fileHash(exe)
		if e != nil {
			return true, e
		}
		return true, rememberPlatformVersion(version, plat, digest)
	}
	return true, nil
}

// Old installations without Go can still seed an rsync transfer from the
// installed binary. Only the verified staging file is passed to install.
func deltaInstall(ctx context.Context, h endpoint, src, digest string) error {
	stage := ".cx-delta-" + digest
	seed := "if ! test -f " + shellQuote(stage) + "; then cp -- " + shellQuote(h.CX) + " " + shellQuote(stage) + "; fi"
	if _, e := h.rawSSH(ctx, "ssh", []string{"--", h.SSH, seed}, "", 30*time.Second); e != nil {
		return e
	}
	if e := rsyncStage(ctx, h, src, stage); e != nil {
		return e
	}
	r, e := h.call(ctx, 30*time.Second, "install", stage, h.CX, digest)
	if e != nil {
		return e
	}
	return r.output()
}

func rsyncStage(ctx context.Context, h endpoint, src, stage string) error {
	opts, e := sshOptions()
	if e != nil {
		return e
	}
	parts := []string{"ssh"}
	for _, v := range opts {
		parts = append(parts, shellQuote(v))
	}
	idle, e := transferSetting("CX_TRANSFER_IDLE", 120*time.Second, false)
	if e != nil {
		return e
	}
	// No -s/--protect-args: openrsync (macOS) lacks it, and newer GNU rsync changed how
	// remote arguments are quoted. Restricting the remote path to shell-safe bytes keeps
	// the remote operand identical under every rsync variant.
	if strings.IndexFunc(stage, func(r rune) bool { return !shellSafe(r) }) >= 0 {
		return fmt.Errorf("rsync stage path needs shell-safe characters: %q", stage)
	}
	cmd := func() error {
		c := exec.CommandContext(ctx, "rsync", "--partial", "--checksum", "--no-whole-file", "--timeout="+strconv.Itoa(max(1, int(idle.Seconds()))), "-e", strings.Join(parts, " "), "--", src, h.SSH+":"+stage)
		c.Stderr = os.Stderr
		err := c.Run()
		var ex *exec.ExitError
		if errors.As(err, &ex) {
			switch ex.ExitCode() {
			case 10, 12, 30, 35, 255:
				return fmt.Errorf("%w: rsync %v", ctlExit(9), err)
			}
		}
		return err
	}
	if e = retryTransfer(ctx, cmd); e != nil {
		return e
	}
	return nil
}

// Seed a staged cross-build from the latest cached binary of the same
// platform. rsync can then send changed blocks even across version keys.
func seedRelayCache(version, plat string) error {
	target, e := cachePath(version, plat)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Dir(target), 0700); e != nil {
		return e
	}
	stage := target + ".part"
	if _, e = os.Stat(stage); e == nil {
		return nil
	} else if !os.IsNotExist(e) {
		return e
	}
	root := filepath.Dir(filepath.Dir(filepath.Dir(target)))
	entries, e := os.ReadDir(root)
	if e != nil {
		return e
	}
	var basis string
	var newest time.Time
	for _, entry := range entries {
		if !entry.IsDir() || entry.Name() == version || !snapHex(entry.Name(), 64) {
			continue
		}
		p, e := cachePath(entry.Name(), plat)
		if e != nil {
			return e
		}
		if st, e := os.Stat(p); e == nil && st.Mode().IsRegular() && st.ModTime().After(newest) {
			newest = st.ModTime()
			basis = p
		}
	}
	if basis != "" {
		return copyFile(basis, stage)
	}
	return nil
}

func shellSafe(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._/-+:@%=,", r)
}
