package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type muxLaunch struct {
	Args []string
	Env  map[string]string
	Log  string
}

func runMuxWorker(ctx context.Context, path string) error {
	var spec muxLaunch
	if e := readJSON(path, &spec); e != nil {
		return e
	}
	if e := os.Remove(path); e != nil {
		return e
	}
	f, e := os.OpenFile(spec.Log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	os.Stdout, os.Stderr = f, f
	for k, v := range spec.Env {
		if e = os.Setenv(k, v); e != nil {
			return e
		}
	}
	return runNode(ctx, spec.Args, true)
}
func launchMuxRunner(ctx context.Context, exe, dir, work string, argv []string, env map[string]string, resume bool) error {
	var before status
	if resume {
		if e := readJSON(filepath.Join(dir, "status.json"), &before); e != nil {
			return e
		}
	}
	var nonce [12]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		return e
	}
	session := fmt.Sprintf("cx-%x", nonce)
	path := filepath.Join(work, "."+session+".json")
	spec := muxLaunch{argv, env, filepath.Join(work, "launch.log")}
	if e := writeJSON(path, spec); e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "psmux", "-L", muxSocket(), "new-session", "-d", "-s", session, "--", exe, "node", "runner-worker", path)
	cmd.Stderr = os.Stderr
	if e := cmd.Run(); e != nil {
		return errors.Join(e, ctx.Err())
	}
	for {
		var s status
		e := readJSON(filepath.Join(dir, "status.json"), &s)
		if e == nil && (!resume || s.LastEvent.After(before.LastEvent) || s.PID != 0 && !isTerminal(s.State)) {
			return json.NewEncoder(os.Stdout).Encode(s)
		}
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		if e = pause(ctx, 30*time.Millisecond); e != nil {
			return fmt.Errorf("runner acknowledgement: %w", e)
		}
	}
}
