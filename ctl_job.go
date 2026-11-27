package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func startNodeJob(ctx context.Context, raw string) error {
	var req jobSubmission
	if e := json.Unmarshal([]byte(raw), &req); e != nil {
		return e
	}
	root, e := stateRoot()
	if e != nil {
		return e
	}
	dir, e := jobDir(root, req.ID)
	if e != nil {
		return e
	}
	work, e := jobDir(filepath.Join(root, ".work"), req.ID)
	if e != nil {
		return e
	}
	bound, e := bindWorkspace(root, req.Source)
	if e != nil {
		return e
	}
	unlock, e := bound.lock(ctx, root, req.ID)
	if e != nil {
		return e
	}
	defer unlock()
	if e = os.MkdirAll(work, 0700); e != nil {
		return e
	}
	receipt := filepath.Join(work, "request.json")
	if _, _, e = jobStatusRecord(dir); e == nil {
		b, receiptErr := readSharedFile(receipt)
		if receiptErr != nil || string(b) != raw {
			return errors.New("job already registered with different input")
		}
		return printStatus(ctx, dir)
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if e = snapImmutable(receipt, []byte(raw)); e != nil {
		return e
	}
	var prep PreparedSnapshot
	if req.Source == "" {
		m, snapshotErr := loadSnapshot(req.Snapshot)
		if snapshotErr != nil {
			return snapshotErr
		}
		prep, snapshotErr = Materialize(ctx, work, m, "")
		if snapshotErr != nil {
			return snapshotErr
		}
	} else {
		prep.Source = bound.path
	}
	if req.Source == "" {
		bound, e = bindWorkspace(root, prep.Source)
		if e != nil {
			return e
		}
	}
	env := prep.Env
	if env == nil {
		env = map[string]string{}
	}
	env["CX_PARENT"] = req.Host + ":" + req.ID
	if len(req.Context) > 0 {
		d := filepath.Join(work, "context")
		for key, b := range req.Context {
			if !snapPath(key) {
				return errors.New("invalid context path")
			}
			path := filepath.Join(d, key)
			if e = snapImmutable(path, b); e != nil {
				return e
			}
		}
		env["CX_CONTEXT"] = d
	}
	argv := []string{"start", "--job", req.ID, "--dir", prep.Source, "--session", req.Session, "--job-host", req.Host}
	argv = append(argv, req.Options...)
	for i := range argv {
		if argv[i] == "--input-json" && i+1 < len(argv) {
			path := filepath.Join(work, "start-input.json")
			if e = snapImmutable(path, []byte(argv[i+1])); e != nil {
				return e
			}
			argv[i+1] = "@" + path
		}
	}

	for _, item := range []struct {
		flag string
		name string
		data []byte
	}{{"--prompt-file", "prompt.txt", []byte(req.Prompt)}, {"--preamble-file", "preamble.md", []byte(req.Preamble)}, {"--stall-nudge-file", "nudge.md", []byte(req.Nudge)}} {
		if len(item.data) > 0 {
			p := filepath.Join(work, item.name)
			if e = snapImmutable(p, item.data); e != nil {
				return e
			}
			argv = append(argv, item.flag, p)
		}
	}
	for i, image := range req.Images {
		p := filepath.Join(work, fmt.Sprintf("image-%d%s", i+1, safeExt(image.Ext)))
		if e = snapImmutable(p, image.Data); e != nil {
			return e
		}
		argv = append(argv, "--image", p)
	}
	if len(req.Command) > 0 {
		argv = append(argv, "--cmd", "--")
		argv = append(argv, req.Command...)
	}
	if e = launchRunner(ctx, root, dir, work, argv, env, jobIdentity{}, bound, unlock, os.Stdout); e != nil {
		// Another submission of the same receipt can win the worker's
		// operation lock after the launcher hands it off.
		identity, current, _, readErr := captureJobIdentity(dir)
		if readErr != nil || current.PID == 0 && !current.final {
			return e
		}
		data, readErr := readSharedFile(receipt)
		if readErr != nil || string(data) != raw || identity.checkDir(dir) != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(current.view(statusTime(ctx)))
	}
	return nil
}

// The runner owns its lifetime. The caller waits only for registration.
func launchRunner(ctx context.Context, root, dir, work string, argv []string, env map[string]string, source jobIdentity, workIdentity workspaceBinding, release func(), out io.Writer) error {
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	token := rand.Text()
	argv = append([]string{argv[0], "--launch-id", token}, argv[1:]...)
	var expected string
	if argv[0] == "resume" {
		expected = source.run
	}
	if source.key != "" {
		argv = append([]string{argv[0], "--expect-run", source.run, "--expect-dir", source.key}, argv[1:]...)
	}
	if workIdentity.key != "" {
		argv = append([]string{argv[0], "--expect-workspace", workIdentity.key}, argv[1:]...)
	}
	if runtime.GOOS == "windows" {
		return launchMuxRunner(ctx, root, exe, dir, work, argv, env, expected, token, release, out)
	}
	log, e := openJobFile(filepath.Join(work, "launch.log"), jobFileAppendCreate)
	if e != nil {
		return e
	}
	defer closeBestEffort(log)
	cmd := exec.Command(exe, append([]string{"node"}, argv...)...)
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if e = startDetachedWorker(cmd); e != nil {
		return e
	}
	// The worker takes the same operation locks and rechecks both identities
	// before it publishes ownership. The acknowledgement comes after that.
	release()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	return awaitRunner(ctx, dir, expected, token, cmd.Process.Pid, done, "", out)
}

// A worker acknowledges ownership with its launch token or, for the command
// handoff, the expected RunID and its first nonzero PID/terminal publication.
func awaitRunner(ctx context.Context, dir, runID, token string, pid int, done <-chan error, rejected string, out io.Writer) error {
	end, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	observer, err := openJobObservation(dir, stateCursor{}, runID)
	if err != nil {
		return err
	}
	defer closeBestEffort(observer)
	var workerErr error
	var exited bool
	for {
		if err := observer.read(); err != nil {
			return err
		}
		s := observer.s
		if runID != "" && s.RunID != "" && s.RunID != runID {
			return errJobReplaced
		}
		if (token != "" && s.LaunchID == token && (s.PID != 0 && (s.PID == pid || pid == 0 || s.Engine == "cmd") || s.final)) ||
			(token == "" && s.RunID == runID && (s.PID != 0 || s.final)) {
			return json.NewEncoder(out).Encode(s.view(statusTime(ctx)))
		}
		if rejected != "" {
			data, err := readSharedFile(rejected)
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			switch string(data) {
			case "owner-busy":
				return errJobOwnerBusy
			case "job-replaced":
				return errJobReplaced
			}
		}
		if exited {
			return errors.Join(errors.New("worker exited before acquiring ownership"), workerErr)
		}
		select {
		case <-end.Done():
			return fmt.Errorf("runner ownership unconfirmed: %w", end.Err())
		case workerErr = <-done:
			exited = true
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func ctlRestart(ctx context.Context, op string, o ctlOptions) error {
	if len(o.pos) != 1 {
		return ctlUsage(op, fmt.Errorf("%s needs a job", op))
	}
	var session string
	if op == "fork" {
		if _, e := jobPath("", o.child); e != nil {
			return ctlUsage(op, e)
		}
		var e error
		session, e = o.submissionSession()
		if e != nil {
			return ctlUsage(op, e)
		}
	}
	hs, e := loadEndpoints()
	if e != nil {
		return e
	}
	host, id, e := resolveJob(ctx, o.pos[0])
	if e != nil {
		return e
	}
	h, e := hs.get(host)
	if e != nil {
		return e
	}
	if op == "resume" {
		if e = forgetResult(host, id); e != nil {
			return e
		}
	}
	argv := []string{"restart-job", op, host, id, o.child, o.through, o.profile, session}
	r, e := h.invoke(ctx, 20*time.Second, false, argv...)
	if e != nil {
		return e
	}
	if r.code != 0 || op == "resume" {
		return r.output()
	}
	p, _, ok := strings.Cut(id, "/")
	child := o.child
	if ok {
		child = p + "/" + child
	}
	if e = rememberRoute(child, host); e != nil {
		return e
	}
	fmt.Println(host + ":" + child)
	return nil
}
func restartNode(ctx context.Context, a []string, expected *jobIdentity, out io.Writer) error {
	if len(a) != 7 {
		return errors.New("invalid restart arguments")
	}
	op, host, id := a[0], a[1], a[2]
	profile, session := a[5], a[6]
	root, e := stateRoot()
	if e != nil {
		return e
	}
	dir, e := jobDir(root, id)
	if e != nil {
		return e
	}
	target := id
	if op == "fork" {
		if _, e = jobPath("", a[3]); e != nil {
			return e
		}
		p, _, ok := strings.Cut(id, "/")
		target = a[3]
		if ok {
			target = p + "/" + target
		}
	}
	identity, s, _, e := captureJobIdentity(dir)
	if e != nil {
		return e
	}
	if expected != nil {
		if *expected != identity {
			return errJobReplaced
		}
		if !isTerminal(s.State) {
			return nil
		}
	}
	work, e := workDir(dir, s)
	if e != nil {
		return e
	}
	saved, prep, e := readRestartInputs(ctx, dir, work, s, expected != nil)
	if e != nil {
		return e
	}
	bound, e := bindWorkspace(root, saved.Dir)
	if e != nil {
		return e
	}
	unlock, e := bound.lock(ctx, root, target, id)
	if e != nil {
		return e
	}
	defer unlock()
	if _, e = readJobStatus(dir, &s); e != nil {
		return e
	}
	if e = identity.check(dir, s); e != nil {
		return e
	}

	if expected != nil && !isTerminal(s.State) {
		return nil
	}
	switch op {
	case "resume":
		if e = checkResumeProfile(root, s.Profile, profile); e != nil {
			return e
		}
	case "fork":
		selected := profile
		if selected == "" {
			selected = s.Profile
		}
		if _, _, e = selectedProfile(root, selected, false); e != nil {
			return e
		}
	}
	argv := []string{"resume", "--job", id}
	env := map[string]string{"CX_PARENT": host + ":" + id}
	if prep != nil {
		var manifest Manifest
		if err := readJSON(filepath.Join(work, "input", "manifest.json"), &manifest); err != nil {
			return err
		}
		if err := manifest.validate(); err != nil {
			return err
		}
		store, err := snapStore()
		if err != nil {
			return err
		}
		if err := validateSnapshotRepos(ctx, store, manifest); err != nil {
			return fmt.Errorf("snapshot repository unavailable: %w", err)
		}
		maps.Copy(env, prep.Env)
	}
	if st, statErr := os.Stat(filepath.Join(work, "context")); statErr == nil && st.IsDir() {
		env["CX_CONTEXT"] = filepath.Join(work, "context")
	}
	if op == "resume" && readyResult(s) {
		if _, e = sealTurn(ctx, dir, s, s.Finished); e != nil {
			return e
		}
	}
	if op == "fork" {
		child := a[3]
		if _, e = jobPath("", child); e != nil {
			return e
		}
		if s.Project != "" {
			child = s.Project + "/" + child
		}
		dir, e = jobDir(root, child)
		if e != nil {
			return e
		}
		if _, e = os.Stat(dir); e == nil {
			return errors.New("fork destination exists")
		} else if !os.IsNotExist(e) {
			return e
		}
		argv = []string{"fork", id, "--job", child, "--job-host", host, "--session", session}
		if a[4] != "" {
			argv = append(argv, "--through", a[4])
		}
	} else if op != "resume" {
		return errors.New("invalid operation")
	}
	if profile != "" {
		argv = append(argv, "--profile", profile)
	}
	return launchRunner(ctx, root, dir, work, argv, env, identity, bound, unlock, out)
}

func continueNode(ctx context.Context, host, id, text, profile string) error {
	root, e := stateRoot()
	if e != nil {
		return e
	}
	dir, e := jobDir(root, id)
	if e != nil {
		return e
	}
	var s status
	if e = projected(ctx, dir, &s); e != nil {
		return e
	}
	if e = checkResumeProfile(root, s.Profile, profile); e != nil {
		return e
	}
	return nodeAction(ctx, []string{"say", id, host, "--cx-text", text})
}

// readRestartInputs checks the saved thread and workspace used by automatic
// resume. Explicit restart may recreate a first turn without a saved thread.
func readRestartInputs(ctx context.Context, dir, work string, s status, requireThread bool) (config, *PreparedSnapshot, error) {
	var saved config
	if requireThread {
		if s.Engine == "cmd" {
			return saved, nil, errors.New("command jobs cannot be replayed by resume")
		}
		if s.Thread == "" {
			return saved, nil, errors.New("job has no saved thread to resume")
		}
	}
	if err := readJobConfig(dir, &saved); err != nil {
		return saved, nil, err
	}

	var prep *PreparedSnapshot
	source := saved.Dir
	if work != dir {
		p, prepared, err := readPreparedSnapshot(ctx, work)
		if err != nil {
			return saved, nil, err
		}
		if prepared {
			prep, source = &p, p.Source
		}
	}
	if source == "" && prep == nil {
		return saved, nil, nil
	}
	info, err := os.Stat(source)
	switch {
	case os.IsNotExist(err) && prep != nil:
		return saved, nil, errors.New("job checkout was reclaimed; submit a new job")
	case err != nil:
		return saved, nil, err
	case !info.IsDir():
		return saved, nil, fmt.Errorf("working directory is not a directory: %s", source)
	default:
		return saved, prep, nil
	}
}
