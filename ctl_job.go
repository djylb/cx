package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
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
	lock, e := jobOperationLock(ctx, root, req.ID)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = os.MkdirAll(work, 0700); e != nil {
		return e
	}
	receipt := filepath.Join(work, "request.json")
	if _, e = os.Stat(filepath.Join(dir, "status.json")); e == nil {
		b, e := os.ReadFile(receipt)
		if e != nil || string(b) != raw {
			return errors.New("job already registered with different input")
		}
		if e = writeJSON(filepath.Join(dir, "cx-work.json"), work); e != nil {
			return e
		}
		return printStatus(dir)
	} else if !os.IsNotExist(e) {
		return e
	}
	if e = snapImmutable(receipt, []byte(raw)); e != nil {
		return e
	}
	var prep PreparedSnapshot
	if req.Source == "" {
		m, e := loadSnapshot(req.Snapshot)
		if e != nil {
			return e
		}
		prep, e = Materialize(ctx, work, m, "")
		if e != nil {
			return e
		}
	} else {
		prep.Source = req.Source
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
	argv := []string{"start", "--job", req.ID, "--dir", prep.Source, "--session", req.Session}
	argv = append(argv, req.Options...)
	for i := range argv {
		if argv[i] == "--input-json" && i+1 < len(argv) {
			path := filepath.Join(work, "start-input.json")
			if e = atomicWrite(path, []byte(argv[i+1])); e != nil {
				return e
			}
			argv[i+1] = "@" + path
		}
	}

	for _, item := range []struct {
		flag string
		name string
		data []byte
	}{{"--prompt-file", "prompt.txt", []byte(req.Prompt)}, {"--preamble-file", "preamble.md", []byte(req.Preamble)}, {"--stall-nudge-file", "nudge.md", []byte(req.Nudge)}, {"--image", "image", req.Image}} {
		if len(item.data) > 0 {
			p := filepath.Join(work, item.name)
			if e = atomicWrite(p, item.data); e != nil {
				return e
			}
			argv = append(argv, item.flag, p)
		}
	}
	if len(req.Command) > 0 {
		argv = append(argv, "--cmd", "--")
		argv = append(argv, req.Command...)
	}
	if e = launchRunner(ctx, dir, work, argv, env, false); e != nil {
		return e
	}
	return writeJSON(filepath.Join(dir, "cx-work.json"), work)
}

// The runner owns its lifetime. The caller waits only for registration.
func launchRunner(ctx context.Context, dir, work string, argv []string, env map[string]string, resume bool) error {
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	if runtime.GOOS == "windows" {
		return launchMuxRunner(ctx, exe, dir, work, argv, env, resume)
	}
	log, e := os.OpenFile(filepath.Join(work, "launch.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer log.Close()
	cmd := exec.Command(exe, append([]string{"node"}, argv...)...)
	detachWorker(cmd)
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.Env = os.Environ()
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if e = cmd.Start(); e != nil {
		return e
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	end, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	for {
		var s status
		e = readJSON(filepath.Join(dir, "status.json"), &s)
		if e == nil && (!resume || s.PID == cmd.Process.Pid) {
			return printStatus(dir)
		}
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		select {
		case <-end.Done():
			return fmt.Errorf("registration unconfirmed: %w", end.Err())
		case err := <-done:
			if err != nil {
				return fmt.Errorf("runner exit: %w (see %s)", err, log.Name())
			}
			return printStatus(dir)
		case <-time.After(30 * time.Millisecond):
		}
	}
}

func restartFlags(op string, child, through, profile *string) *flag.FlagSet {
	fs := cliFlags(op)
	fs.StringVar(profile, "profile", "", "execution host profile")
	if op == "fork" {
		fs.StringVar(child, "job", "", "required child job tag")
		fs.StringVar(through, "through", "", "last turn to include (defaults to all turns)")
	}
	return fs
}

func ctlRestart(ctx context.Context, op string, a []string) error {
	if len(a) == 0 || strings.HasPrefix(a[0], "-") {
		return ctlUsage(op, fmt.Errorf("%s needs a job", op))
	}
	child, through, profile := "", "", ""
	fs := restartFlags(op, &child, &through, &profile)
	if e := fs.Parse(a[1:]); e != nil {
		return ctlUsage(op, e)
	}
	if fs.NArg() > 0 {
		return ctlUsage(op, errors.New("unexpected arguments"))
	}
	hs, e := loadEndpoints()
	if e != nil {
		return e
	}
	host, id, e := resolveJob(ctx, a[0])
	if e != nil {
		return e
	}
	h, e := hs.get(host)
	if e != nil {
		return e
	}
	if op == "fork" {
		if _, e = jobPath("", child); e != nil {
			return e
		}
	}
	if op == "resume" {
		if e = forgetResult(host, id); e != nil {
			return e
		}
	}
	argv := []string{"restart-job", op, host, id, child, through}
	if profile != "" {
		argv = append(argv, profile)
	}
	r, e := h.invoke(ctx, 20*time.Second, false, argv...)
	if e != nil {
		return e
	}
	if r.code != 0 || op == "resume" {
		return r.output()
	}
	p, _, ok := strings.Cut(id, "/")
	if ok {
		child = p + "/" + child
	}
	if e = rememberRoute(child, host); e != nil {
		return e
	}
	fmt.Println(host + ":" + child)
	return nil
}
func restartNode(ctx context.Context, a []string) error {
	if len(a) != 5 && len(a) != 6 {
		return errors.New("invalid restart arguments")
	}
	op, host, id := a[0], a[1], a[2]
	var profile string
	if len(a) == 6 {
		profile = a[5]
	}
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
	var s status
	if e = readJSON(filepath.Join(dir, "status.json"), &s); e != nil {
		return e
	}
	work, e := workDir(dir, s)
	if e != nil {
		return e
	}
	workspaceID := privateWorkspaceID(root, work)
	if workspaceID != "" && workspaceID != target {
		lock, err := jobOperationLock(ctx, root, workspaceID)
		if err != nil {
			return err
		}
		defer lock.Close()
	}
	lock, e := jobOperationLock(ctx, root, target)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = readJSON(filepath.Join(dir, "status.json"), &s); e != nil {
		return e
	}
	if op == "resume" {
		if e = checkResumeProfile(root, s.Profile, profile); e != nil {
			return e
		}
	} else if op == "fork" {
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
	var prep PreparedSnapshot
	if e = readJSON(filepath.Join(work, "prepared.json"), &prep); e == nil {
		if _, err := os.Stat(prep.Source); os.IsNotExist(err) {
			return errors.New("job checkout was reclaimed; submit a new job")
		} else if err != nil {
			return err
		}
		for k, v := range prep.Env {
			env[k] = v
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	if st, e := os.Stat(filepath.Join(work, "context")); e == nil && st.IsDir() {
		env["CX_CONTEXT"] = filepath.Join(work, "context")
	}
	if op == "resume" && readyResult(s) {
		if _, e = sealTurn(ctx, dir, s, s.Finished); e != nil {
			return e
		}
	}
	resume := true
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
		argv = []string{"fork", id, "--job", child, "--session", host + ":" + id}
		if a[4] != "" {
			argv = append(argv, "--through", a[4])
		}
		resume = false
	} else if op != "resume" {
		return errors.New("invalid operation")
	}
	if profile != "" {
		argv = append(argv, "--profile", profile)
	}
	return launchRunner(ctx, dir, work, argv, env, resume)
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
	if isTerminal(s.State) && s.Thread != "" {
		if e = restartNode(ctx, []string{"resume", host, id, "", ""}); e != nil {
			return e
		}
	}
	return nodeAction(ctx, []string{"say", id, "--cx-text", text})
}
