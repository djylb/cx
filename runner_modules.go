package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/djylb/timeshim"
)

type acceptanceResult struct {
	Module    string   `json:"module"`
	Cmd       []string `json:"cmd"`
	Exit      int64    `json:"exit"`
	Duration  int64    `json:"duration"` // milliseconds
	Tail      string   `json:"output_tail"`
	Truncated bool     `json:"truncated"`
}

func runModuleCommand(ctx context.Context, clock timeshim.Clock, c *config, job string, argv []string, shell bool, dir string, env []string, timeout time.Duration, poll func() error, stdout, stderr io.Writer) (int64, error) {
	if len(argv) == 0 || strings.TrimSpace(argv[0]) == "" {
		return -1, errors.New("empty command")
	}
	if poll != nil {
		if err := poll(); err != nil {
			return -1, err
		}
	}
	cmd := commandArgv(argv, shell)
	cmd.Dir = dir
	c.mark(cmd, job)
	cmd.Env = append(slices.DeleteFunc(cmd.Env, func(entry string) bool {
		key, _, _ := strings.Cut(entry, "=")
		return gitWorkspaceEnv(entry) || strings.EqualFold(key, "CX_CONTEXT") || strings.EqualFold(key, "GOWORK")
	}), env...)
	out, _ := c.secret.output(cmd, stdout)
	cmd.Stdout, cmd.Stderr = out, out
	if stderr != nil {
		cmd.Stderr, _ = c.secret.output(cmd, stderr)
	}
	code, err := runBoundedCommand(ctx, clock, timeout, cmd, poll, c)
	if err != nil && code == 0 {
		code = -1
	}
	return code, err
}

var errAcceptanceInterrupted = errors.New("acceptance interrupted")

func (r *runner) pollModuleCommand() error {
	if _, closing, err := r.takeMarker("close"); err != nil {
		return err
	} else if closing {
		return errQueuedClose
	}
	if _, stop, err := r.takeMarker("interrupt"); err != nil {
		return err
	} else if stop {
		return errAcceptanceInterrupted
	}
	return nil
}

func (r *runner) modulePreamble() string {
	work := r.c.Work
	if work == nil {
		return ""
	}
	var note []string
	if len(work.Modules) > 0 {
		note = append(note, fmt.Sprintf("Declared modules: %s. Explain in your report why you read any files outside the declared module scope.", strings.Join(work.Modules, ", ")))
	}
	if work.Changeset != "" {
		note = append(note, fmt.Sprintf("Include the Git commit trailer Changeset: %s in every commit for this job.", work.Changeset))
	}
	var generated map[string]bool
	if work.hasModuleContext() && r.closeRequest == 0 {
		var err error
		generated, err = r.prepareModuleContexts()
		if err != nil {
			_ = r.note("module/context", err.Error())
		}
	}
	for _, module := range work.Definitions {
		if len(module.Deps) > 0 {
			note = append(note, fmt.Sprintf("Direct dependencies of %s: %s.", module.Name, strings.Join(module.Deps, ", ")))
		}
		if len(module.Context) == 0 || r.closeRequest != 0 {
			continue
		}
		if !generated[module.Name] {
			note = append(note, fmt.Sprintf("Context package for %s failed to generate. See the job progress log.", module.Name))
		} else {
			note = append(note, fmt.Sprintf("Context package for %s: $CX_CONTEXT/modules/%s/%s.md.", module.Name, r.moduleContextVersion, module.Name))
		}
	}
	return strings.Join(note, "\n")
}

func (r *runner) prepareModuleContexts() (map[string]bool, error) {
	contextDir, err := r.moduleContextDir()
	if err != nil {
		return nil, err
	}
	work, err := workDir(r.dir, r.s)
	if err != nil {
		return nil, err
	}
	base, err := readJobBaseline(work)
	if err != nil {
		return nil, err
	}
	if base.Snapshot == "" {
		var req jobSubmission
		if err = readJSON(filepath.Join(work, "request.json"), &req); err != nil {
			return nil, err
		}
		base.Snapshot = req.Snapshot
	}
	version := string(base.Snapshot)
	if base.Request != "" {
		version = base.Request
	}
	if !snapPath(version) || strings.Contains(version, "/") {
		return nil, errors.New("missing or invalid context baseline")
	}
	parent := filepath.Join(contextDir, "modules")
	folder := filepath.Join(parent, version)
	published := make(map[string]bool)
	for _, module := range r.c.Work.Definitions {
		if _, statErr := os.Stat(filepath.Join(folder, module.Name+".md")); statErr == nil {
			published[module.Name] = true
		}
	}
	if st, statErr := os.Stat(folder); statErr == nil && st.IsDir() {
		if syncErr := syncJobTree(folder, contextDir); syncErr != nil {
			return nil, syncErr
		}
		r.moduleContextVersion = version
		return published, nil
	}
	if err = os.MkdirAll(parent, 0700); err != nil {
		return nil, err
	}
	stage, err := os.MkdirTemp(parent, ".stage-")
	if err != nil {
		return nil, err
	}
	defer removeTreeBestEffort(stage)
	for _, module := range r.c.Work.Definitions {
		if len(module.Context) == 0 {
			continue
		}
		if err := r.pollModuleCommand(); err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(r.baseContext(), moduleSetupTimeout)
		scratch, err := r.prepareModuleInput(ctx, cancel, base.Snapshot)
		if err != nil {
			cancel()
			_ = r.note("module/context", fmt.Sprintf("%s: %v", module.Name, err))
			continue
		}
		var stdout bytes.Buffer
		budget := &budgetOutput{dst: &stdout, left: moduleOutputBudget(r.c.Work.ContextOutputBytes)}
		var diag diagnosticTail
		code, err := runModuleCommand(ctx, r.clock, &r.c, r.dir, module.Context, false, r.c.Work.repoDir(scratch.cwd), scratch.env,
			moduleSetupTimeout, r.pollModuleCommand, budget, &diag)
		cancel()
		removeTreeBestEffort(scratch.dir)
		if errors.Is(err, errQueuedClose) || errors.Is(err, errAcceptanceInterrupted) {
			return nil, err
		}
		if err != nil {
			message := fmt.Sprintf("%s exit=%d: %v", module.Name, code, err)
			if !errors.Is(err, errOutputTruncated) && len(diag.tail) != 0 {
				message += " " + string(diag.tail)
			}
			_ = r.note("module/context", message)
			continue
		}
		if budget.truncated {
			stdout.WriteString("\n[Context output truncated]\n")
		}
		if err := publishImmutable(filepath.Join(stage, module.Name+".md"), stdout.Bytes(), stage); err != nil {
			return nil, err
		}
		published[module.Name] = true
		_ = r.note("module/context", fmt.Sprintf("%s exit=%d %s", module.Name, code, diag.tail))
	}
	if err := syncJobDir(stage); err != nil {
		return nil, err
	}
	if err := os.Rename(stage, folder); err != nil {
		return nil, err
	}
	if err := syncJobTree(parent, contextDir); err != nil {
		return nil, err
	}
	r.moduleContextVersion = version
	return published, nil
}

// Preparation owns private directories, so a close can return immediately
// while the canceled preparation exits and cleans up its own output.
func (r *runner) prepareModuleInput(ctx context.Context, cancel context.CancelFunc, snapshot SnapshotID) (moduleScratch, error) {
	type prepared struct {
		scratch moduleScratch
		err     error
	}
	completed := make(chan prepared)
	job := r.dir
	go func() {
		scratch, err := prepareAuxiliaryInput(ctx, job, snapshot, "", nil)
		select {
		case completed <- prepared{scratch, err}:
		case <-ctx.Done():
			removeTreeBestEffort(scratch.dir)
		}
	}()
	ticker := timeshim.NewTicker(timeshim.Or(r.clock), commandPollInterval)
	defer ticker.Stop()
	for {
		select {
		case done := <-completed:
			return done.scratch, done.err
		case <-ctx.Done():
			return moduleScratch{}, ctx.Err()
		case <-ticker.C:
			if err := r.pollModuleCommand(); err != nil {
				cancel()
				return moduleScratch{}, err
			}
		}
	}
}

func (r *runner) moduleContextDir() (string, error) {
	work, err := workDir(r.dir, r.s)
	if err != nil {
		return "", err
	}
	dir := filepath.Join(work, "context")
	if r.c.Env == nil {
		r.c.Env = map[string]string{}
	}
	r.c.Env["CX_CONTEXT"] = dir
	return dir, nil
}

type diagnosticTail struct {
	tail      []byte
	truncated bool
}

func (w *diagnosticTail) Write(p []byte) (int, error) {
	const limit = 600 // Leaves room for the module name, exit code and truncation marker.
	w.truncated = w.truncated || len(w.tail)+len(p) > limit
	if len(p) >= limit {
		w.tail = append(w.tail[:0], p[len(p)-limit:]...)
	} else {
		if overflow := len(w.tail) + len(p) - limit; overflow > 0 {
			w.tail = append(w.tail[:0], w.tail[overflow:]...)
		}
		w.tail = append(w.tail, p...)
	}
	return len(p), nil
}
