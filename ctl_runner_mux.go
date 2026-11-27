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
	"slices"
	"strings"
	"time"
)

type muxLaunch struct {
	Env    map[string]string
	Window *muxWindow
	Log    string
	Args   []string
}

const muxCleanupTimeout = 5 * time.Second

type muxWindow struct {
	Program string `json:"program"`
	Socket  string `json:"socket"`
	Session string `json:"session"`
	Name    string `json:"name"`
}

func (w *muxWindow) launchArgs(argv ...string) []string {
	if w.Session == w.Name {
		return append([]string{"-L", w.Socket, "new-session", "-d", "-s", w.Session, "--"}, argv...)
	}
	return append([]string{"-L", w.Socket, "new-window", "-d", "-t", w.Session, "-n", w.Name, "--"}, argv...)
}

func (w *muxWindow) close(ctx context.Context) error {
	if w == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), muxCleanupTimeout)
	defer cancel()
	// Address only the window IDs that still match this launch. psmux can
	// resolve a missing name against its current session or use a prefix.
	list := func() ([]string, error) {
		cmd := exec.CommandContext(ctx, w.Program, "-L", w.Socket, "list-windows", "-a",
			"-F", "#{window_id}\t#{session_name}\t#{window_name}")
		cmd.WaitDelay = time.Second
		out, err := cmd.Output()
		if err != nil {
			return nil, errors.Join(err, ctx.Err())
		}
		var ids []string
		for line := range strings.Lines(string(out)) {
			id, target, ok := strings.Cut(strings.TrimSpace(line), "\t")
			if ok && strings.HasPrefix(id, "@") &&
				(target == w.Session+"\t"+w.Name || w.Session == w.Name && strings.HasPrefix(target, w.Session+"\t")) {
				ids = append(ids, id)
			}
		}
		return ids, nil
	}
	ids, err := list()
	if err != nil {
		return fmt.Errorf("list psmux windows for %s: %w", w.Name, err)
	}
	for _, id := range ids {
		if err = exec.CommandContext(ctx, w.Program, "-L", w.Socket, "kill-window", "-t", id).Run(); err != nil {
			remaining, listErr := list()
			if listErr != nil || slices.Contains(remaining, id) {
				return fmt.Errorf("close psmux window %s %s: %w", w.Name, id, errors.Join(err, listErr))
			}
		}
	}
	return nil
}

type muxWorkerKey struct{}

func workerWindow(ctx context.Context) *muxWindow {
	w, _ := ctx.Value(muxWorkerKey{}).(*muxWindow)
	return w
}

func runMuxWorker(ctx context.Context, path string) (result error) {
	var spec muxLaunch
	if e := readJSON(path, &spec); e != nil {
		return e
	}
	if runtime.GOOS == "windows" && spec.Window != nil {
		ctx = context.WithValue(ctx, muxWorkerKey{}, spec.Window)
		defer func() { result = errors.Join(result, spec.Window.close(ctx)) }()
	}
	f, e := openJobFile(spec.Log, jobFileAppendCreate)
	if e != nil {
		return e
	}
	defer closeBestEffort(f)
	os.Stdout, os.Stderr = f, f
	for k, v := range spec.Env {
		if e = os.Setenv(k, v); e != nil {
			return e
		}
	}
	err := runNode(ctx, spec.Args, true)
	var rejection string
	switch {
	case errors.Is(err, errJobOwnerBusy):
		rejection = "owner-busy"
	case errors.Is(err, errJobReplaced):
		rejection = "job-replaced"
	}
	if rejection != "" {
		// The launcher can already have removed this launch's directory.
		if writeErr := writeJobBytes(path+".rejected", []byte(rejection)); !os.IsNotExist(writeErr) {
			return errors.Join(err, writeErr)
		}
	}
	return err
}
func launchMuxRunner(ctx context.Context, root, exe, dir, work string, argv []string, env map[string]string, runID, token string, release func(), out io.Writer) error {
	window, err := newMuxWindow(root)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	path, dispose, err := startMuxWorker(ctx, window, root, exe, work, argv, env)
	if err != nil {
		return err
	}
	defer dispose()
	release()
	return awaitRunner(ctx, dir, runID, token, 0, nil, path+".rejected", out)
}

func newMuxWindow(root string) (*muxWindow, error) {
	var nonce [12]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		return nil, e
	}
	window := psmuxLaunch(root, fmt.Sprintf("cx-%x", nonce))
	return &window, nil
}

func startMuxWorker(ctx context.Context, window *muxWindow, root, exe, work string, argv []string, env map[string]string) (path string, dispose func(), result error) {
	attempts := filepath.Join(work, ".cx-launches")
	// A previous launcher may still be waiting for its acknowledgement.
	// Each attempt owns its descriptor and removes it after waiting.
	attempt := filepath.Join(attempts, window.Name)
	if err := os.MkdirAll(attempt, 0700); err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(attempt) }
	defer func() {
		if result != nil {
			cleanup()
		}
	}()
	path = filepath.Join(attempt, "worker.json")
	// The session belongs to the existing psmux server, which may have
	// started with a different CXRUN_STATE than this request.
	workerEnv := make(map[string]string, len(env)+1)
	maps.Copy(workerEnv, env)
	workerEnv["CXRUN_STATE"] = root
	spec := muxLaunch{Args: argv, Env: workerEnv, Log: filepath.Join(work, "launch.log"), Window: window}
	data, err := json.Marshal(spec)
	if err != nil {
		return "", nil, err
	}
	if err = snapImmutable(path, data); err != nil {
		return "", nil, err
	}
	cmd := exec.CommandContext(ctx, window.Program, window.launchArgs(exe, "node", "runner-worker", path)...)
	cmd.Stderr = os.Stderr
	if e := cmd.Run(); e != nil {
		return "", nil, errors.Join(e, ctx.Err())
	}
	return path, cleanup, nil
}
