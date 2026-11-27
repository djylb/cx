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
	"strconv"
	"strings"
	"time"

	"github.com/djylb/timeshim"
)

type turnPatch struct {
	*ResultBase
	Source    string `json:"source,omitempty"`
	mergeBase string // reconstructed for an incremental application
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

type resultUnavailable string

func (e resultUnavailable) Error() string { return string(e) }
func (e resultUnavailable) Unwrap() error { return ctlExit(44) }

func unsealedTurn(n int64) error {
	return resultUnavailable(fmt.Sprintf("turn %d not sealed yet", n))
}

func currentTurnID(s status) string {
	if s.Turn != "" {
		return s.Turn
	}
	return s.LastCompleted
}

func resultInProgress(s status) bool {
	return !isTerminal(s.State) && (s.TurnStatus == "inProgress" || s.State == "running" ||
		s.State == "queued" || s.State == "waiting_approval" ||
		s.State == "waiting_input" && s.InputKind == "server_request")
}

func readyResult(s status) bool {
	if resultInProgress(s) {
		return false
	}
	if s.Finished == 0 {
		return s.State != "waiting"
	}
	return s.LastCompleted != "" && currentTurnID(s) == s.LastCompleted
}

func canSealResult(s status) bool {
	// Admission can queue the next turn before it starts. Its runner seals
	// the prior turn before allowing any further worktree changes.
	return readyResult(s) || s.State == "queued" && s.TurnStatus != "inProgress" &&
		s.LastCompleted != "" && currentTurnID(s) == s.LastCompleted
}

func resultCheckout(work string) (string, error) {
	if _, err := os.Stat(filepath.Join(work, "input", "manifest.json")); err == nil {
		return work, nil
	} else if !os.IsNotExist(err) {
		return "", err
	}
	return "", nil
}

func (r *runner) sealSnapshotTurn() error {
	if r.s.Finished == 0 || !readyResult(r.s) {
		return nil
	}
	work, err := workDir(r.dir, r.s)
	if err != nil {
		return err
	}
	checkout, err := resultCheckout(work)
	if err != nil || checkout == "" {
		return err
	}
	_, err = sealTurn(r.ctx, r.dir, r.s, r.s.Finished)
	return err
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
	var current status
	if e = loadJobStatusInto(dir, &current); e != nil {
		return "", e
	}
	if s.RunID != current.RunID {
		return "", errJobReplaced
	}
	s = current
	if _, e = os.Stat(filepath.Join(folder, "result.json")); e == nil {
		return folder, nil
	} else if !os.IsNotExist(e) {
		return "", e
	}
	if n > s.Finished {
		return "", unsealedTurn(n)
	}
	if n < s.Finished {
		return "", resultUnavailable(fmt.Sprintf("turn %d was not sealed before the job advanced", n))
	}
	missingReport := resultUnavailable(fmt.Sprintf("turn %d report missing", n))
	if n > 0 && s.LastCompleted == "" && !resultInProgress(s) {
		return "", missingReport
	}
	if !canSealResult(s) {
		return "", unsealedTurn(n)
	}
	report, e := readTurnReport(ctx, dir, s.LastCompleted)
	if os.IsNotExist(e) || e == nil && report == nil && s.LastCompleted != "" {
		return "", missingReport
	}
	if e != nil {
		return "", e
	}
	var result SnapshotResult
	checkout, e := resultCheckout(work)
	if e != nil {
		return "", e
	}
	if checkout != "" {
		result, e = Collect(ctx, checkout)
	} else {
		result.Dir, e = os.MkdirTemp(work, ".seal-")
		if e == nil {
			defer removeTreeBestEffort(result.Dir)
			var c config
			if e = readJobConfig(dir, &c); e == nil && c.Dir != "" {
				e = copySnapshotArtifacts(filepath.Join(c.Dir, "artifacts"), &result)
			}
		}
	}
	if e != nil {
		return "", e
	}
	defer removeTreeBestEffort(result.Dir)
	// last.md is the report entry in the sealed result archive.
	if e = writeJobBytes(filepath.Join(result.Dir, "last.md"), report); e != nil {
		return "", e
	}
	meta := turnResult{Turn: n, TurnID: s.LastCompleted, Repositories: []turnPatch{}, Bases: map[string]ResultBase{}}
	if s.Engine != "cmd" {
		meta.Outcome = s.CompletedStatus
	}
	if checkout != "" {
		var manifest Manifest
		if err := readJSON(filepath.Join(checkout, "input", "manifest.json"), &manifest); err != nil {
			return "", err
		}
		var req jobSubmission
		if err := readJSON(filepath.Join(work, "request.json"), &req); err != nil && !os.IsNotExist(err) {
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
	}
	if result.Artifacts == nil {
		result.Artifacts = []SnapshotArtifact{}
	}
	meta.Artifacts = result.Artifacts
	data, e := json.Marshal(meta)
	if e != nil {
		return "", e
	}
	if e = snapImmutable(filepath.Join(result.Dir, "result.json"), data); e != nil {
		return "", e
	}
	var after status
	if _, e = readJobStatus(dir, &after); e != nil {
		return "", e
	}
	if s.Finished != after.Finished || s.Turn != after.Turn || s.LastCompleted != after.LastCompleted ||
		s.CompletedStatus != after.CompletedStatus || s.RunID != after.RunID || !canSealResult(after) {
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
	f, e := createJobTemp(filepath.Dir(path), ".archive-")
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
		in, e := openJobFile(p, jobFileRead)
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
	if _, e = readJobStatus(dir, &s); e != nil {
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
	f, e := openJobFile(archive, jobFileRead)
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

func resultTurnCompleted(s status, n int64) bool {
	return n <= s.Finished && (s.Finished > 0 || readyResult(s))
}

func nodeObserveResult(ctx context.Context, a []string) error {
	if len(a) != 3 {
		return errors.New("invalid result observation arguments")
	}
	n, err := strconv.ParseInt(a[2], 10, 64)
	if err != nil || n < 0 {
		return errors.New("invalid result turn")
	}
	root, err := stateRoot()
	if err != nil {
		return err
	}
	dir, err := jobDir(root, a[0])
	if err != nil {
		return err
	}
	observer, err := openJobObservation(dir, stateCursor{}, "")
	if err != nil {
		return err
	}
	defer closeBestEffort(observer)
	for {
		if observer.s.RunID != a[1] {
			return errJobReplaced
		}
		if resultTurnCompleted(observer.s, n) || observer.s.final {
			return json.NewEncoder(os.Stdout).Encode(observer.s)
		}
		if err = pause(ctx, 200*time.Millisecond); err == nil {
			err = observer.poll(ctx)
		}
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return errObserveTimeout
			}
			return err
		}
	}
}

type resultReader struct {
	ctx context.Context
	io.Reader
}

func (r resultReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.Reader.Read(p)
}

func validateResultTurn(dir string, target observedResult) error {
	var m turnResult
	if e := readJSON(filepath.Join(dir, "result.json"), &m); e != nil {
		return e
	}
	if m.Turn != target.Number || target.TurnID != "" && m.TurnID != target.TurnID {
		return errors.New("result manifest does not match the requested turn")
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
	return nil
}

func unpackTurn(ctx context.Context, archive, dest string, target observedResult) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	f, e := openJobFile(archive, jobFileRead)
	if e != nil {
		return e
	}
	defer closeBestEffort(f)
	z, e := gzip.NewReader(resultReader{ctx, f})
	if e != nil {
		return e
	}
	defer closeBestEffort(z)
	reader := resultReader{ctx, z}
	r := tar.NewReader(reader)
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
		out, unpackErr := openJobFile(p, jobFileNew)
		if unpackErr != nil {
			return unpackErr
		}
		_, unpackErr = io.Copy(out, r)
		if unpackErr = errors.Join(unpackErr, out.Close()); unpackErr != nil {
			return unpackErr
		}
	}
	if _, e = io.Copy(io.Discard, reader); e != nil {
		return e
	}
	if e = validateResultTurn(tmp, target); e != nil {
		return e
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	return os.Rename(tmp, dest)
}

func fetchTurn(ctx context.Context, h endpoint, id, gen, turn, dest string, details bool) error {
	n, e := strconv.ParseInt(turn, 10, 64)
	if e != nil || n < 0 {
		return errors.New("invalid result turn")
	}
	return fetchResultTurn(ctx, h, id, dest, details, observedResult{RunID: gen, Number: n})
}

func fetchResultTurn(ctx context.Context, h endpoint, id, dest string, details bool, target observedResult) error {
	ctx, cancel := timeshim.WithTimeout(ctx, timeshim.Or(contextClock(ctx)), collectLimit)
	defer cancel()
	return collectResultTurn(ctx, h, id, dest, details, target)
}

func collectResultTurn(ctx context.Context, h endpoint, id, dest string, details bool, target observedResult) error {
	lock, e := transferLock(ctx, dest+".lock")
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	if e = validateResultTurn(dest, target); !os.IsNotExist(e) {
		return e
	}
	partial := dest + ".part"
	f, e := openJobFile(partial, jobFileAppendCreate)
	if e != nil {
		return e
	}
	defer closeBestEffort(f)
	st, e := f.Stat()
	if e != nil {
		return e
	}
	var total int64
	if st.Size() == 0 || readJSON(dest+".total", &total) != nil || total != st.Size() {
		if e = downloadTurn(ctx, h, id, dest, details, target, f, st.Size()); e != nil {
			return e
		}
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = installResultTurn(ctx, partial, dest, target); e != nil {
		return e
	}
	removeBestEffort(dest + ".total")
	return nil
}

func installResultTurn(ctx context.Context, partial, dest string, target observedResult) error {
	if e := unpackTurn(ctx, partial, dest, target); e != nil {
		if !errors.Is(e, context.Canceled) && !errors.Is(e, context.DeadlineExceeded) {
			removeBestEffort(partial)
		}
		return e
	}
	return os.Remove(partial)
}

func downloadTurn(ctx context.Context, h endpoint, id, dest string, details bool, target observedResult, f *os.File, offset int64) error {
	idle, e := transferSetting("CX_TRANSFER_IDLE", 120*time.Second, false)
	if e != nil {
		return e
	}
	attempt, stop := context.WithCancel(ctx)
	defer stop()
	args := []string{"result-download", id, target.RunID, strconv.FormatInt(target.Number, 10), strconv.FormatInt(offset, 10)}
	if details {
		args = append(args, "details")
	}
	cmd, e := h.transportCommand(attempt, args, idle)
	if e != nil {
		return e
	}
	sink := resultStream{file: f, offset: offset, total: -1, progress: dest + ".total"}
	var diag bytes.Buffer
	if e = cmd.run(ctx, stop, &sink, &diag); e != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(e, errSSHMasterUnsupported) {
			return e
		}
		var ex *exec.ExitError
		if errors.As(e, &ex) && ex.ExitCode() >= 0 && ex.ExitCode() != 255 {
			if ex.ExitCode() == 44 {
				reason := strings.TrimPrefix(strings.TrimSpace(cleanDiag(diag.Bytes())), "cx: ")
				if reason == "" {
					reason = fmt.Sprintf("turn %d unavailable on %s", target.Number, id)
				}
				return resultUnavailable(reason)
			}
			diagnosticf("%s", cleanDiag(diag.Bytes()))
			return ctlExit(ex.ExitCode())
		}
		return fmt.Errorf("%w: %v %s", ctlExit(9), e, cleanDiag(diag.Bytes()))
	}
	if sink.total < 0 || sink.offset != sink.total {
		return fmt.Errorf("%w: interrupted result transfer", ctlExit(9))
	}
	return nil
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
func selectResultTurn(ctx context.Context, h endpoint, id, turn string, wait bool) (status, observedResult, error) {
	var target observedResult
	if turn != "" {
		n, err := strconv.ParseInt(turn, 10, 64)
		if err != nil || n < 0 {
			return status{}, target, errors.New("invalid result turn")
		}
		target.Number = n
	}
	r, err := h.call(ctx, 30*time.Second, "status", "--job", id)
	if err != nil {
		return status{}, target, err
	}
	if r.code != 0 {
		return status{}, target, r.output()
	}
	var s status
	if err = json.Unmarshal(r.data, &s); err != nil {
		return s, target, err
	}
	target.RunID = s.RunID
	if turn == "" {
		target.Number = s.Finished
		switch {
		case resultInProgress(s):
			target.Number++
		case !readyResult(s) && (!wait || s.LastCompleted == ""):
			return s, target, resultUnavailable("no completed turn selected by the job")
		}
	}
	if !resultTurnCompleted(s, target.Number) {
		if !wait {
			return s, target, unsealedTurn(target.Number)
		}
		if !isTerminal(s.State) {
			deadline, _ := ctx.Deadline()
			r, err = h.call(ctx, deadline.Sub(timeshim.Or(contextClock(ctx)).Now()), "observe-result", id, target.RunID, strconv.FormatInt(target.Number, 10))
			if err != nil {
				return s, target, err
			}
			if r.code != 0 {
				return s, target, r.output()
			}
			s = status{}
			if err = json.Unmarshal(r.data, &s); err != nil {
				return s, target, err
			}
			if s.RunID != target.RunID {
				return s, target, errJobReplaced
			}
		}
		if !resultTurnCompleted(s, target.Number) {
			return s, target, resultUnavailable(fmt.Sprintf("turn %d not sealed (job is %s)", target.Number, s.State))
		}
	}
	if turn == "" && target.Number == s.Finished {
		target.TurnID = s.LastCompleted
	}
	return s, target, nil
}

func ctlResult(ctx context.Context, host, id, op string, o ctlOptions) (err error) {
	wait := op == "result" && o.wait
	if op == "result" {
		limit, e := observation(o.timeout)
		if e != nil {
			return e
		}
		var cancel context.CancelFunc
		ctx, cancel = timeshim.WithTimeout(ctx, timeshim.Or(contextClock(ctx)), limit)
		defer cancel()
		defer func() {
			if err != nil && ctx.Err() != nil && !errors.Is(err, ctlExit(44)) {
				err = ctx.Err()
			}
		}()
		if wait {
			if handled, e := observeSubmission(ctx, host, id, "wait", limit); handled || e != nil {
				return e
			}
		}
	}
	fetch := fetchResultTurn
	if op == "result" && o.timeout != "" {
		// The explicit command deadline also bounds collection.
		fetch = collectResultTurn
	}
	details := o.json || o.log
	if op == "result" || op == "patch" {
		if o.turn != "" && !o.refresh && !o.incremental {
			cached, e := cachedResultDir(host, id, o.turn, details, o.path)
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
	publish := op == "result" || op == "agent" || op == "patch"
	var marker string
	var seen resultReference
	if publish {
		marker, seen, e = resultMarkerSnapshot(host, id)
		if e != nil {
			return e
		}
	}
	s, target, e := selectResultTurn(ctx, h, id, o.turn, wait)
	if e != nil {
		return e
	}
	n := target.Number
	var since int64
	if o.incremental {
		since = n
		if since == 0 {
			return errors.New("--incremental requires a positive completed turn")
		}
	} else if o.sinceTurn != "" {
		since, e = strconv.ParseInt(o.sinceTurn, 10, 64)
		if e != nil || since < 1 || since > n {
			return fmt.Errorf("--since-turn must be between 1 and the latest completed turn (%d)", n)
		}
	}
	turn := strconv.FormatInt(n, 10)
	dest, e := resultDest(host, id, s.RunID, turn)
	if e != nil {
		return e
	}
	dest = resultVariant(dest, details)
	if publish {
		if e = publishResultMarker(ctx, marker, seen, s.RunID, s.Finished); e != nil {
			return e
		}
	}
	if o.path {
		cached, err := cachedResultReport(dest, target)
		if err != nil {
			return err
		}
		if cached != "" {
			return displayTurn(ctx, cached, id, op, o)
		}
	}
	if _, e = os.Stat(filepath.Join(dest, "result.json")); os.IsNotExist(e) {
		if e = fetch(ctx, h, id, dest, details, target); e != nil {
			if op == "result" && !errors.Is(e, ctlExit(44)) {
				return resultUnavailable(fmt.Sprintf("collect turn %d for %s: %v", n, id, e))
			}
			return e
		}
	} else if e != nil {
		return e
	} else if o.turn == "" {
		if e = validateResultTurn(dest, target); e != nil {
			return e
		}
	}
	if op == "apply" || since > 1 {
		var m turnResult
		if e = readJSON(filepath.Join(dest, "result.json"), &m); e != nil {
			return e
		}
		patchDir := dest
		if since > 1 {
			previous, err := resultDest(host, id, s.RunID, strconv.FormatInt(since-1, 10))
			if err != nil {
				return err
			}
			if err = fetchTurn(ctx, h, id, s.RunID, strconv.FormatInt(since-1, 10), previous, false); err != nil {
				if errors.Is(err, ctlExit(44)) {
					return resultUnavailable(fmt.Sprintf("Sealed turn %d is unavailable for %s (%v). Use the cumulative patch or cx rebase %s TEXT.", since-1, id, err, id))
				}
				return err
			}
			patchDir, err = os.MkdirTemp("", "cx-incremental-")
			if err != nil {
				return err
			}
			defer removeTreeBestEffort(patchDir)
			m, err = incrementalResult(ctx, previous, dest, patchDir, since-1, m)
			if err != nil {
				return err
			}
		}
		if op == "apply" {
			return applyTurn(ctx, dest, patchDir, id, host, target.RunID, since, o, m)
		}
		return displayTurn(ctx, patchDir, id, op, o)
	}
	return displayTurn(ctx, dest, id, op, o)
}
func displayTurn(ctx context.Context, dir, id, op string, o ctlOptions) error {
	if o.path {
		path, err := resultReportPath(dir)
		if err != nil {
			return err
		}
		if err := checkResultReport(path); err != nil {
			return resultUnavailable(err.Error())
		}
		_, err = fmt.Fprintln(os.Stdout, path)
		return err
	}
	if o.json || o.log {
		return displayResultDetails(os.Stdout, dir, id, o.log)
	}
	if op == "result" || op == "patch" && !o.full {
		var b bytes.Buffer
		e := writeTurn(ctx, &b, dir, id, op, o)
		if e != nil && !errors.Is(e, ctlExit(1)) {
			return e
		}
		if previewErr := preview(b.Bytes(), o); previewErr != nil {
			return errors.Join(previewErr, e)
		}
		if op == "result" && !o.full {
			report, pathErr := resultReportPath(dir)
			if pathErr != nil {
				return errors.Join(pathErr, e)
			}
			diagnosticf("cx: summary shown; full report: %s (use --full)\n", report)
		}
		return e
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
				b, e := readSharedFile(p)
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
	b, e := readSharedFile(p)
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
	r, e := h.call(ctx, d+time.Second, "observe-one", id, d.String())
	if e != nil {
		return e
	}
	if r.code == exitQueued {
		return printQueuedWait(ctx, h, id, r)
	}
	var notice waitObservation
	if r.code == 6 {
		if e = json.Unmarshal(r.data, &notice); e != nil {
			return e
		}
		if notice.InputKind == "server_request" {
			if _, e = fmt.Println("STATUS: QUESTION"); e != nil {
				return e
			}
			if e = json.NewEncoder(os.Stdout).Encode(notice.Approvals); e != nil {
				return e
			}
			if _, e = fmt.Printf("JOB=%s STATUS=QUESTION patch=none repos=0\n", id); e != nil {
				return e
			}
			return acknowledgeWait(ctx, h, id, notice, r.code)
		}
	}
	if r.code == 124 {
		return r.output()
	}
	if e = ctlResult(ctx, host, id, "agent", ctlOptions{wait: true, full: full}); e != nil {
		return e
	}
	if r.code == 6 {
		return acknowledgeWait(ctx, h, id, notice, r.code)
	}
	if r.code != 0 {
		return ctlExit(r.code)
	}
	return nil
}

// result-download seals a ready turn under the result lock before streaming it.
// An unavailable selected turn (44) gives this observer no reason to poll again.
const prefetchLimit = 2 * time.Second

func prefetchResult(ctx context.Context, h endpoint, host, id, marker string, seen resultReference, target observedResult) {
	ctx, cancel := timeshim.WithTimeout(ctx, timeshim.Or(contextClock(ctx)), prefetchLimit)
	defer cancel()
	if ctx.Err() != nil {
		return
	}
	dest, e := resultDest(host, id, target.RunID, strconv.FormatInt(target.Number, 10))
	if e != nil {
		return
	}
	if e = fetchResultTurn(ctx, h, id, resultVariant(dest, false), false, target); e == nil {
		_ = publishResultMarker(ctx, marker, seen, target.RunID, target.Number)
	}
}

type resultReference struct {
	Generation string
	Turn       int64
	Epoch      uint64
}

func resultMarker(host, id string) (string, error) {
	d, e := resultDest(host, id, "", "0")
	if e != nil {
		return "", e
	}
	return filepath.Join(filepath.Dir(d), "latest.json"), nil
}
func readResultReference(path string) (resultReference, error) {
	var ref resultReference
	e := readJSON(path, &ref)
	if os.IsNotExist(e) {
		e = nil
	}
	return ref, e
}

func resultMarkerSnapshot(host, id string) (string, resultReference, error) {
	path, e := resultMarker(host, id)
	if e != nil {
		return "", resultReference{}, e
	}
	ref, e := readResultReference(path)
	return path, ref, e
}

func publishResultMarker(ctx context.Context, path string, seen resultReference, gen string, turn int64) error {
	lock, e := lockUntil(ctx, path+".lock")
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	old, e := readResultReference(path)
	if e != nil {
		return e
	}
	// Invalidation fences writers that started before it. A new generation can
	// replace only the marker observed before the status or wait request.
	if old.Epoch != seen.Epoch || (old.Generation != gen && old != seen) ||
		(old.Generation == gen && old.Turn >= turn) {
		return nil
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	return writeJSON(path, resultReference{Generation: gen, Turn: turn, Epoch: old.Epoch})
}

func forgetResult(host, id string) error {
	path, e := resultMarker(host, id)
	if e != nil {
		return e
	}
	lock, e := lockUntil(context.Background(), path+".lock")
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	old, e := readResultReference(path)
	if e != nil {
		return e
	}
	return writeJSON(path, resultReference{Epoch: old.Epoch + 1})
}

func cachedResultDir(host, id, turn string, details, reportOnly bool) (string, error) {
	_, ref, e := resultMarkerSnapshot(host, id)
	if e != nil {
		return "", e
	}
	if ref.Generation == "" {
		return "", nil
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
	if reportOnly {
		return cachedResultReport(d, observedResult{RunID: ref.Generation, Number: n})
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
