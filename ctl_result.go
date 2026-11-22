package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type turnPatch struct {
	*ResultBase
	Source string `json:"source,omitempty"`
	SnapshotPatch
}

type ResultBase struct {
	Base   *string `json:"base"`
	Branch *string `json:"branch"`
}

type turnResult struct {
	*ResultDetails
	Bases        map[string]ResultBase `json:"bases"`
	TurnID       string                `json:"turn_id"`
	Outcome      string                `json:"outcome,omitempty"`
	Repositories []turnPatch           `json:"repositories"`
	Artifacts    []SnapshotArtifact    `json:"artifacts"`
	Turn         int64                 `json:"turn"`
}

func readyResult(s status) bool {
	if s.State == "running" || s.State == "waiting_approval" || s.State == "queued" || s.State == "waiting_input" && s.InputKind == "server_request" {
		return false
	}
	if s.Finished == 0 {
		return s.State != "waiting"
	}
	return s.Turn == s.LastCompleted
}
func sealTurn(ctx context.Context, dir string, s status, n int64) (string, error) {
	work, e := workDir(dir, s)
	if e != nil {
		return "", e
	}
	base := filepath.Join(work, "result")
	if e = os.MkdirAll(base, 0700); e != nil {
		return "", e
	}
	folder := filepath.Join(base, fmt.Sprintf("turn-%d", n))
	lock, e := lockUntil(ctx, filepath.Join(base, ".lock"))
	if e != nil {
		return "", e
	}
	defer closeBestEffort(lock)
	if _, e = os.Stat(filepath.Join(folder, "result.json")); e == nil {
		return folder, nil
	} else if !os.IsNotExist(e) {
		return "", e
	}
	if n != s.Finished || !readyResult(s) {
		return "", ctlExit(44)
	}
	var result SnapshotResult
	checkout := work
	if _, e = os.Stat(filepath.Join(work, "jobs", "checkout", "input", "manifest.json")); e == nil {
		checkout = filepath.Join(work, "jobs", "checkout")
	} else if !os.IsNotExist(e) {
		return "", e
	}
	if _, e = os.Stat(filepath.Join(checkout, "input", "manifest.json")); e == nil {
		result, e = Collect(ctx, checkout)
	} else if os.IsNotExist(e) {
		result.Dir, e = os.MkdirTemp(work, ".seal-")
		if e == nil {
			defer removeTreeBestEffort(result.Dir)
			var c config
			if e = readJSON(filepath.Join(dir, "config.json"), &c); e == nil && c.Dir != "" {
				e = copySnapshotArtifacts(filepath.Join(c.Dir, "artifacts"), &result)
			} else if os.IsNotExist(e) {
				e = nil
			}
		}
	}
	if e != nil {
		return "", e
	}
	defer removeTreeBestEffort(result.Dir)
	if e = copyFile(filepath.Join(dir, "last.md"), filepath.Join(result.Dir, "last.md")); e != nil {
		if !os.IsNotExist(e) || s.Finished > 0 {
			return "", e
		}
		if e = atomicWrite(filepath.Join(result.Dir, "last.md"), nil); e != nil {
			return "", e
		}
	}
	meta := turnResult{Turn: n, TurnID: s.LastCompleted, Repositories: []turnPatch{}, Bases: map[string]ResultBase{}}
	if s.Engine != "cmd" {
		meta.Outcome = s.CompletedStatus
	}
	{
		var manifest Manifest
		if err := readJSON(filepath.Join(checkout, "input", "manifest.json"), &manifest); err == nil {
			var req jobSubmission
			if err = readJSON(filepath.Join(work, "request.json"), &req); err != nil && !os.IsNotExist(err) {
				return "", err
			}
			for _, r := range manifest.Repositories {
				if r.Head != nil {
					meta.Bases[r.Path] = ResultBase{Base: r.Head, Branch: req.Branches[r.Path]}
				}
			}
			for _, r := range result.Repositories {
				var provenance *ResultBase
				if v, ok := meta.Bases[r.Path]; ok {
					provenance = &v
				}
				meta.Repositories = append(meta.Repositories, turnPatch{SnapshotPatch: r, Source: req.Sources[r.Path], ResultBase: provenance})
			}
		} else if !os.IsNotExist(err) {
			return "", err
		}
	}
	if result.Artifacts == nil {
		result.Artifacts = []SnapshotArtifact{}
	}
	meta.Artifacts = result.Artifacts
	if e = writeJSON(filepath.Join(result.Dir, "result.json"), meta); e != nil {
		return "", e
	}
	var after status
	if e = readJSON(filepath.Join(dir, "status.json"), &after); e != nil {
		return "", e
	}
	beforeReport, e := os.ReadFile(filepath.Join(result.Dir, "last.md"))
	if e != nil {
		return "", e
	}
	currentReport, e := os.ReadFile(filepath.Join(dir, "last.md"))
	if e != nil && (!os.IsNotExist(e) || s.Finished != 0) {
		return "", e
	}
	if !bytes.Equal(beforeReport, currentReport) {
		return "", errors.New("report changed during collection")
	}
	if s.Finished != after.Finished || s.Turn != after.Turn || s.RunID != after.RunID || !readyResult(after) {
		return "", errors.New("job changed during collection")
	}
	if e = os.Rename(result.Dir, folder); e != nil {
		return "", e
	}
	return folder, nil
}

func resultArchive(ctx context.Context, folder, dir string, s status, details bool) (string, error) {
	path := resultVariant(folder, details) + ".tar.gz"
	if _, e := os.Stat(path); e == nil {
		return path, nil
	}
	var extra string
	var progress receiveProgress
	if details {
		var e error
		extra, e = os.MkdirTemp(filepath.Dir(folder), ".result-details-")
		if e != nil {
			return "", e
		}
		defer removeTreeBestEffort(extra)
		if e = stageResultDetails(ctx, dir, folder, extra, s, &progress); e != nil {
			return "", e
		}
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".archive-")
	if e != nil {
		return "", e
	}
	defer removeBestEffort(f.Name())
	z := gzip.NewWriter(f)
	w := tar.NewWriter(z)
	var payload io.Writer = w
	if details {
		payload = io.MultiWriter(w, &progress)
	}
	add := func(root, p string, d fs.DirEntry, e error) error {
		if e != nil || d.IsDir() {
			return e
		}
		if !d.Type().IsRegular() {
			return errors.New("unexpected result entry")
		}
		st, e := d.Info()
		if e != nil {
			return e
		}
		rel, e := filepath.Rel(root, p)
		if e != nil {
			return e
		}
		if e = w.WriteHeader(&tar.Header{Name: filepath.ToSlash(rel), Mode: 0600, Size: st.Size(), Typeflag: tar.TypeReg}); e != nil {
			return e
		}
		in, e := os.Open(p)
		if e != nil {
			return e
		}
		defer closeBestEffort(in)
		_, e = io.Copy(payload, in)
		return e
	}
	manifestPath := filepath.Join(folder, "result.json")
	e = filepath.WalkDir(folder, func(p string, d fs.DirEntry, walkErr error) error {
		if details && p == manifestPath {
			return walkErr
		}
		return add(folder, p, d, walkErr)
	})
	if e == nil && details {
		e = filepath.WalkDir(extra, func(p string, d fs.DirEntry, walkErr error) error {
			return add(extra, p, d, walkErr)
		})
	}
	if e = errors.Join(e, w.Close(), z.Close(), f.Close()); e != nil {
		return "", e
	}
	if e = os.Link(f.Name(), path); e != nil && !os.IsExist(e) {
		return "", e
	}
	return path, nil
}
func nodeResult(ctx context.Context, a []string) error {
	details, e := resultDetailsArg(a)
	if e != nil {
		return e
	}
	n, e := strconv.ParseInt(a[2], 10, 64)
	if e != nil || n < 0 {
		return errors.New("invalid result turn")
	}
	offset, e := strconv.ParseInt(a[3], 10, 64)
	if e != nil || offset < 0 {
		return errors.New("invalid result offset")
	}
	root, e := stateRoot()
	if e != nil {
		return e
	}
	dir, e := jobDir(root, a[0])
	if e != nil {
		return e
	}
	var s status
	if e = readJSON(filepath.Join(dir, "status.json"), &s); e != nil {
		return e
	}
	if s.RunID != a[1] {
		return errors.New("job generation changed")
	}
	folder, e := sealTurn(ctx, dir, s, n)
	if e != nil {
		return e
	}
	archive, e := resultArchive(ctx, folder, dir, s, details)
	if e != nil {
		return e
	}
	f, e := os.Open(archive)
	if e != nil {
		return e
	}
	defer closeBestEffort(f)
	st, e := f.Stat()
	if e != nil {
		return e
	}
	if offset > st.Size() {
		return errors.New("invalid result offset")
	}
	_, e = f.Seek(offset, io.SeekStart)
	if e != nil {
		return e
	}
	if _, e = fmt.Fprintln(os.Stdout, st.Size()); e != nil {
		return e
	}
	_, e = io.Copy(os.Stdout, f)
	return e
}
func unpackTurn(archive, dest string) error {
	f, e := os.Open(archive)
	if e != nil {
		return e
	}
	defer closeBestEffort(f)
	z, e := gzip.NewReader(f)
	if e != nil {
		return e
	}
	defer closeBestEffort(z)
	r := tar.NewReader(z)
	tmp, e := os.MkdirTemp(filepath.Dir(dest), ".unpack-")
	if e != nil {
		return e
	}
	defer removeTreeBestEffort(tmp)
	for {
		h, unpackErr := r.Next()
		if unpackErr == io.EOF {
			break
		}
		if unpackErr != nil {
			return unpackErr
		}
		if h.Typeflag != tar.TypeReg || !snapPath(h.Name) {
			return errors.New("invalid result archive entry")
		}
		p := filepath.Join(tmp, h.Name)
		if unpackErr = os.MkdirAll(filepath.Dir(p), 0700); unpackErr != nil {
			return unpackErr
		}
		out, unpackErr := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if unpackErr != nil {
			return unpackErr
		}
		_, unpackErr = io.Copy(out, r)
		if unpackErr = errors.Join(unpackErr, out.Close()); unpackErr != nil {
			return unpackErr
		}
	}
	if _, e = io.Copy(io.Discard, z); e != nil {
		return e
	}
	var m turnResult
	if e = readJSON(filepath.Join(tmp, "result.json"), &m); e != nil {
		return e
	}
	for _, row := range m.Repositories {
		if !snapPath(row.Patch) {
			return errors.New("invalid patch path")
		}
	}
	for _, art := range m.Artifacts {
		if !snapPath(art.Path) {
			return errors.New("invalid artifact path")
		}
	}
	return os.Rename(tmp, dest)
}

func fetchTurn(ctx context.Context, h endpoint, id, gen, turn, dest string, details bool) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	lock, e := transferLock(ctx, dest+".lock")
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	if _, e = os.Stat(filepath.Join(dest, "result.json")); e == nil {
		return nil
	}
	partial := dest + ".part"
	f, e := os.OpenFile(partial, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		return e
	}
	defer closeBestEffort(f)
	st, e := f.Stat()
	if e != nil {
		return e
	}
	idle, e := transferSetting("CX_TRANSFER_IDLE", 120*time.Second, false)
	if e != nil {
		return e
	}
	attempt, stop := context.WithCancel(ctx)
	defer stop()
	args := []string{"result-download", id, gen, turn, strconv.FormatInt(st.Size(), 10)}
	if details {
		args = append(args, "details")
	}
	cmd, e := h.command(attempt, args)
	if e != nil {
		return e
	}
	sink := resultStream{file: f, offset: st.Size(), total: -1, progress: dest + ".total"}
	var diag bytes.Buffer
	if e = runActive(cmd, stop, h.connectionBudget(idle), idle, &sink, &diag); e != nil {
		var ex *exec.ExitError
		if errors.As(e, &ex) && ex.ExitCode() >= 0 && ex.ExitCode() != 255 {
			fmt.Fprint(os.Stderr, cleanDiag(diag.Bytes()))
			return ctlExit(ex.ExitCode())
		}
		if runtime.GOOS == "windows" && unsupportedDiagnostic(runtime.GOOS, diag.String()) {
			unsupportedMaster.Store(true)
		}
		return fmt.Errorf("%w: %v %s", ctlExit(9), e, cleanDiag(diag.Bytes()))
	}
	if sink.total < 0 || sink.offset != sink.total {
		return fmt.Errorf("%w: interrupted result transfer", ctlExit(9))
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = unpackTurn(partial, dest); e != nil {
		_ = os.Remove(partial)
		return e
	}
	return os.Remove(partial)
}

func resultDest(host, id, gen, turn string) (string, error) {
	base := os.Getenv("CX_HOME")
	if base == "" {
		d, e := os.UserCacheDir()
		if e != nil {
			return "", e
		}
		base = filepath.Join(d, "cx")
	}
	p, e := jobPath(filepath.Join(base, "out", ".hosts"), host)
	if e != nil {
		return "", e
	}
	p, e = jobDir(p, id)
	if e != nil {
		return "", e
	}
	if gen != "" {
		p, e = jobPath(p, gen)
		if e != nil {
			return "", e
		}
	}
	p = filepath.Join(p, "result")
	if e = os.MkdirAll(p, 0700); e != nil {
		return "", e
	}
	return filepath.Join(p, "turn-"+turn), nil
}
func ctlResult(ctx context.Context, host, id, op string, o ctlOptions) error {
	details := o.json || o.log
	if op == "result" || op == "patch" {
		if !o.refresh {
			cached, e := cachedResultDir(host, id, o.turn, details)
			if e != nil {
				return e
			}
			if cached != "" {
				return displayTurn(ctx, cached, id, op, o)
			}
		}
	}
	hs, e := loadEndpoints()
	if e != nil {
		return e
	}
	h, e := hs.get(host)
	if e != nil {
		return e
	}
	r, e := h.call(ctx, 30*time.Second, "status", "--job", id)
	if e != nil {
		return e
	}
	if r.code != 0 {
		return r.output()
	}
	var s status
	if e = json.Unmarshal(r.data, &s); e != nil {
		return e
	}
	turn := o.turn
	if turn == "" {
		turn = strconv.FormatInt(s.Finished, 10)
	}
	n, e := strconv.ParseInt(turn, 10, 64)
	if e != nil || n < 0 {
		return errors.New("invalid result turn")
	}
	turn = strconv.FormatInt(n, 10)
	dest, e := resultDest(host, id, s.RunID, turn)
	if e != nil {
		return e
	}
	dest = resultVariant(dest, details)
	if op == "result" || op == "agent" || op == "patch" {
		marker, markerErr := resultMarker(host, id)
		if markerErr != nil {
			return markerErr
		}
		if markerErr = writeJSON(marker, resultReference{s.RunID, s.Finished}); markerErr != nil {
			return markerErr
		}
	}
	if _, e = os.Stat(filepath.Join(dest, "result.json")); os.IsNotExist(e) {
		if n > s.Finished || (n == s.Finished && !readyResult(s) && n == 0) {
			return ctlExit(44)
		}
		if op == "result" && !o.wait && (details || s.CompletedStatus != "failed") {
			if !o.refresh {
				var failure fetchFailure
				if e = readJSON(dest+".error", &failure); e == nil && failure.Code != 9 && (failure.Code != 44 || !readyResult(s)) {
					fmt.Fprintln(os.Stderr, failure.Message)
					return ctlExit(failure.Code)
				}
			}
			return backgroundFetch(host, id, s.RunID, turn, dest, details)
		}
		if e = fetchTurn(ctx, h, id, s.RunID, turn, dest, details); e != nil {
			if op == "agent" && errors.Is(e, ctlExit(9)) {
				return backgroundFetch(host, id, s.RunID, turn, dest, false)
			}
			return e
		}
	} else if e != nil {
		return e
	}
	if op == "apply" || op == "rebase" {
		var m turnResult
		if e = readJSON(filepath.Join(dest, "result.json"), &m); e != nil {
			return e
		}
		if op == "rebase" {
			return rebaseTurn(ctx, dest, host, id, o.dir, m)
		}
		return applyTurn(ctx, dest, id, host, o, m)
	}
	return displayTurn(ctx, dest, id, op, o)
}
func workerFetch(ctx context.Context, a []string) error {
	details, e := resultDetailsArg(a)
	if e != nil {
		return e
	}
	n, e := strconv.ParseUint(a[3], 10, 63)
	if e != nil {
		return e
	}
	t := strconv.FormatUint(n, 10)
	dest, e := resultDest(a[0], a[1], a[2], t)
	if e != nil {
		return e
	}
	dest = resultVariant(dest, details)
	hs, e := loadEndpoints()
	if e != nil {
		return e
	}
	h, e := hs.get(a[0])
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	l, e := lockPath(dest+".worker", os.O_CREATE)
	if e != nil || l == nil {
		return e
	}
	defer closeBestEffort(l)
	e = retryTransfer(ctx, func() error { return fetchTurn(ctx, h, a[1], a[2], t, dest, details) })
	if e != nil {
		code := ctlExit(1)
		errors.As(e, &code)
		return errors.Join(e, writeJSON(dest+".error", fetchFailure{Code: int(code), Message: e.Error()}))
	}
	if err := os.Remove(dest + ".error"); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
func displayTurn(ctx context.Context, dir, id, op string, o ctlOptions) error {
	if o.json || o.log {
		return displayResultDetails(os.Stdout, dir, id, o.log)
	}
	if op == "result" || op == "patch" && !o.full {
		var b bytes.Buffer
		e := writeTurn(ctx, &b, dir, id, op, o)
		if e != nil && !errors.Is(e, ctlExit(1)) {
			return e
		}
		return errors.Join(preview(b.Bytes(), o), e)
	}
	return writeTurn(ctx, os.Stdout, dir, id, op, o)
}
func writeTurn(ctx context.Context, out io.Writer, dir, id, op string, o ctlOptions) error {
	var m turnResult
	if e := readJSON(filepath.Join(dir, "result.json"), &m); e != nil {
		return e
	}
	if op == "patch" {
		var rows []turnPatch
		for _, row := range m.Repositories {
			if o.repo == "" || o.repo == row.Path || o.repo == row.RepoID {
				rows = append(rows, row)
			}
		}
		if o.repo != "" && len(rows) != 1 {
			return errors.New("select a unique changed repository")
		}
		for _, row := range rows {
			p := filepath.Join(dir, row.Patch)
			if o.repo == "" {
				if _, e := fmt.Fprintf(out, "# repo: %s\n", row.Path); e != nil {
					return e
				}
			}
			if o.full {
				b, e := os.ReadFile(p)
				if e != nil {
					return e
				}
				if _, e = out.Write(b); e != nil {
					return e
				}
			} else {
				cmd := exec.CommandContext(ctx, "git", "apply", "--stat", p)
				cmd.Stdout = out
				cmd.Stderr = os.Stderr
				if e := cmd.Run(); e != nil {
					return e
				}
			}
		}
		return nil
	}
	p := filepath.Join(dir, "last.md")
	b, e := os.ReadFile(p)
	if e != nil {
		return e
	}
	report := string(b)
	if !o.full {
		decisions := ""
		if span := decisionHeading.FindStringIndex(report); span != nil {
			end := len(report)
			if next := nextHeading.FindStringIndex(report[span[1]:]); next != nil {
				end = span[1] + next[0]
			}
			decisions = report[span[0]:end]
			report = report[:span[0]]
		}
		lines := strings.Split(report, "\n")
		lines = lines[:min(10, len(lines))]
		blank := false
		for i, line := range lines {
			if blank && strings.HasPrefix(line, "## ") {
				lines = lines[:i]
				break
			}
			blank = blank || strings.TrimSpace(line) == ""
		}
		report = strings.TrimSpace(strings.Join(lines, "\n"))
		if decisions != "" {
			report += "\n\n" + decisions
		}
	}
	if op != "agent" {
		_, _ = fmt.Fprintln(out, p)
	}
	_, _ = fmt.Fprintln(out, strings.TrimSuffix(report, "\n"))
	if op == "agent" {
		_, _ = fmt.Fprintln(out, p)
	}
	for _, a := range m.Artifacts {
		_, _ = fmt.Fprintf(out, "%s (%d bytes)\n", filepath.Join(dir, a.Path), a.Bytes)
	}
	if op == "agent" {
		patch := "none"
		if len(m.Repositories) > 0 {
			patch = filepath.Join(dir, "result.json")
		}
		first, _, _ := strings.Cut(report, "\n")
		state := reportStatus(first)
		if state == "" {
			state = "UNKNOWN"
		}
		_, _ = fmt.Fprintf(out, "JOB=%s STATUS=%s patch=%s repos=%d\n", id, state, patch, len(m.Repositories))
	} else if o.full {
		_, _ = fmt.Fprintln(out, filepath.Join(dir, "result.json"))
		_, _ = fmt.Fprintf(out, "%d changed repositories\n", len(m.Repositories))
		for _, r := range m.Repositories {
			_, _ = fmt.Fprintf(out, "%s: %d files changed, %d bytes (%s)\n", r.Path, r.Files, r.Bytes, r.Patch)
		}
	}
	if op == "result" && m.Outcome == "failed" {
		return ctlExit(1)
	}
	return nil
}
func agentReport(ctx context.Context, host string, h endpoint, id string, full bool) error {
	d, e := observation("")
	if e != nil {
		return e
	}
	r, e := h.call(ctx, d+time.Second, "wait", "--job", id, "--timeout", d.String())
	if e != nil {
		return e
	}
	if r.code == 6 {
		snapshot, statusErr := h.call(ctx, 30*time.Second, "status", "--job", id)
		if statusErr != nil {
			return statusErr
		}
		if snapshot.code != 0 {
			return snapshot.output()
		}
		var s status
		if statusErr = json.Unmarshal(snapshot.data, &s); statusErr != nil {
			return statusErr
		}
		if s.InputKind == "server_request" {
			fmt.Println("STATUS: QUESTION")
			if statusErr = json.NewEncoder(os.Stdout).Encode(s.Approvals); statusErr != nil {
				return statusErr
			}
			fmt.Printf("JOB=%s STATUS=QUESTION patch=none repos=0\n", id)
			return ctlExit(6)
		}
	}
	if r.code == 124 {
		return r.output()
	}
	if e = ctlResult(ctx, host, id, "agent", ctlOptions{wait: true, full: full}); e != nil {
		return e
	}
	if r.code != 0 {
		return ctlExit(r.code)
	}
	return nil
}

type fetchFailure struct {
	Message string
	Code    int
}

func backgroundFetch(host, id, gen, turn, dest string, details bool) error {
	lock, e := lockPath(dest+".worker", os.O_CREATE)
	if e != nil {
		return e
	}
	if lock != nil {
		exe, e := os.Executable()
		if e != nil {
			_ = lock.Close()
			return e
		}
		args := []string{"node", "fetch-turn", host, id, gen, turn}
		if details {
			args = append(args, "details")
		}
		cmd := exec.Command(exe, args...)
		detachWorker(cmd)
		_ = lock.Close()
		if e = cmd.Start(); e != nil {
			return e
		}
		go func() { _ = cmd.Wait() }()
	}
	count := int64(0)
	if st, e := os.Stat(dest + ".part"); e == nil {
		count = st.Size()
	}
	var total int64
	size := "?"
	if readJSON(dest+".total", &total) == nil {
		size = strconv.FormatInt(total, 10)
	}
	if details {
		fmt.Fprintf(os.Stderr, "collecting turn=%s bytes=%d/%s\n", turn, count, size)
	} else {
		fmt.Printf("collecting turn=%s bytes=%d/%s\n", turn, count, size)
	}
	return ctlExit(12)
}

type resultReference struct {
	Generation string
	Turn       int64
}

func resultMarker(host, id string) (string, error) {
	d, e := resultDest(host, id, "", "0")
	if e != nil {
		return "", e
	}
	return filepath.Join(filepath.Dir(d), "latest.json"), nil
}
func forgetResult(host, id string) error {
	path, e := resultMarker(host, id)
	if e != nil {
		return e
	}
	e = os.Remove(path)
	if os.IsNotExist(e) {
		return nil
	}
	return e
}
func cachedResultDir(host, id, turn string, details bool) (string, error) {
	p, e := resultMarker(host, id)
	if e != nil {
		return "", e
	}
	var ref resultReference
	if e = readJSON(p, &ref); os.IsNotExist(e) {
		return "", nil
	} else if e != nil {
		return "", e
	}
	n := ref.Turn
	if turn != "" {
		n, e = strconv.ParseInt(turn, 10, 64)
		if e != nil || n < 0 {
			return "", errors.New("invalid result turn")
		}
	}
	d, e := resultDest(host, id, ref.Generation, strconv.FormatInt(n, 10))
	if e != nil {
		return "", e
	}
	d = resultVariant(d, details)
	if _, e = os.Stat(filepath.Join(d, "result.json")); os.IsNotExist(e) {
		return "", nil
	} else if e != nil {
		return "", e
	}
	return d, nil
}

// The size line and archive share stdout. Preserve every received archive byte
// even when SSH returns an error; the next attempt advertises that exact offset.
type resultStream struct {
	file          *os.File
	progress      string
	header        []byte
	offset, total int64
}

func (s *resultStream) Write(b []byte) (int, error) {
	n := len(b)
	if s.total < 0 {
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			if len(s.header)+len(b) > 24 {
				return 0, errors.New("invalid result header")
			}
			s.header = append(s.header, b...)
			return n, nil
		}
		if len(s.header)+i > 24 {
			return 0, errors.New("invalid result header")
		}
		line := append(s.header, b[:i]...)
		total, e := strconv.ParseInt(string(line), 10, 64)
		if e != nil || total <= 0 || total < s.offset {
			return 0, errors.New("invalid result size")
		}
		s.total = total
		if e = writeJSON(s.progress, total); e != nil {
			return 0, e
		}
		b = b[i+1:]
	}
	if int64(len(b)) > s.total-s.offset {
		return 0, errors.New("excess result data")
	}
	count, e := s.file.Write(b)
	s.offset += int64(count)
	if e != nil {
		return 0, e
	}
	return n, nil
}
