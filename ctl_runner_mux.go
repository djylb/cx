package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type muxLaunch struct {
	Env  map[string]string
	Log  string
	Args []string
}

func runMuxWorker(ctx context.Context, path string) error {
	var spec muxLaunch
	if e := readJSON(path, &spec); e != nil {
		return e
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
func launchMuxRunner(ctx context.Context, root, exe, dir, work string, argv []string, env map[string]string, runID, token string, release func()) error {
	var nonce [12]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		return e
	}
	session := fmt.Sprintf("cx-%x", nonce)
	attempts := filepath.Join(work, ".cx-launches")
	// A previous launcher may still be waiting for its acknowledgement.
	// Each attempt owns its descriptor and removes it after waiting.
	attempt := filepath.Join(attempts, session)
	if err := os.MkdirAll(attempt, 0700); err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(attempt) }()
	path := filepath.Join(attempt, "worker.json")
	// The session belongs to the existing psmux server, which may have
	// started with a different CXRUN_STATE than this request.
	workerEnv := make(map[string]string, len(env)+1)
	maps.Copy(workerEnv, env)
	workerEnv["CXRUN_STATE"] = root
	spec := muxLaunch{Args: argv, Env: workerEnv, Log: filepath.Join(work, "launch.log")}
	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	if err = snapImmutable(path, data); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	program, socket := psmuxSettings(root)
	cmd := exec.CommandContext(ctx, program, "-L", socket, "new-session", "-d", "-s", session, "--", exe, "node", "runner-worker", path)
	cmd.Stderr = os.Stderr
	if e := cmd.Run(); e != nil {
		return errors.Join(e, ctx.Err())
	}
	release()
	return awaitRunner(ctx, dir, runID, token, 0, nil, path+".rejected")
}
