package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"
)

type process struct {
	Command    string `json:"command_line"`
	PID        int    `json:"pid"`
	PPID       int    `json:"ppid"`
	Start      uint64 `json:"start_time"` // platform-native process creation timestamp
	InTree     bool   `json:"in_runner_tree"`
	Unreadable bool   `json:"metadata_unreadable,omitempty"`
	Marked     bool   `json:"run_marked"`
	jobObject  bool
}
type inventory struct {
	scanErr    error
	Warning    string    `json:"warning,omitempty"`
	Processes  []process `json:"processes"`
	Unreadable int       `json:"unreadable"`
}

func (c config) mark(cmd *exec.Cmd, dir string) {
	env := cmd.Environ()
	for k, v := range c.Env {
		env = append(env, k+"="+v)
	}
	env = append(env, c.localEnv...)
	cmd.Env = append(env, "CXRUN_JOB="+c.Project+"/"+filepath.Base(dir), "CXRUN_RUN="+c.RunID)
}
func sameProcess(start, current uint64) bool { return start != 0 && start == current }

func (r *runner) inventory() inventory {
	v := inventory{Processes: []process{}}
	if r.c.RunID == "" {
		v.Warning = "legacy job has no recorded process identity"
		return v
	}
	all := scanProcesses(r.c.RunID, &v)
	root := r.s.PID
	for _, p := range all {
		if p.PID == root && sameProcess(r.s.RunnerStart, p.Start) {
			continue
		}
		// Every link belongs to the same snapshot. Guard against cycles and
		// parent PID reuse while the platform process table was being read.
		for a, depth := p, 0; a.PID > 0 && depth < len(all); depth++ {
			if a.PID == root && sameProcess(r.s.RunnerStart, a.Start) {
				p.InTree = true
				break
			}
			parent, ok := all[a.PPID]
			if !ok || parent.Start > a.Start || parent.PID == a.PID {
				break
			}
			a = parent
		}
		if p.Marked || p.jobObject || p.InTree {
			v.Processes = append(v.Processes, p)
		}
	}
	sort.Slice(v.Processes, func(i, j int) bool { return v.Processes[i].PID < v.Processes[j].PID })
	return v
}
func printProcesses(dir string) error {
	r := runner{dir: dir}
	if err := readJSON(filepath.Join(dir, "config.json"), &r.c); err != nil {
		return err
	}
	if err := readJSON(filepath.Join(dir, "status.json"), &r.s); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(r.inventory())
}

// Only an explicit close or clean calls reclaim. Tree-only matches belong
// in ps, but cannot authorize a signal.
func (r *runner) reclaim() error {
	if r.c.RunID == "" {
		return nil
	}
	if r.log == nil {
		f, err := os.OpenFile(filepath.Join(r.dir, "progress.log"), os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer closeBestEffort(f)
		r.log = f
		defer func() { r.log = nil }()
	}
	type identity struct {
		pid   int
		start uint64
	}
	sent := map[identity]time.Time{}
	deadline := time.Now().Add(30 * time.Second)
	var logErr error
	for {
		view := r.inventory()
		if view.scanErr != nil {
			return errors.Join(logErr, view.scanErr, r.note("process/reap", view.scanErr.Error()))
		}
		active := 0

		for _, p := range view.Processes {
			if !p.Marked {
				continue
			}
			active++
			key := identity{p.PID, p.Start}
			since, already := sent[key]
			force := already && time.Since(since) >= 10*time.Second
			if time.Now().After(deadline) || (already && time.Since(since) > 11*time.Second) {
				return errors.Join(logErr, fmt.Errorf("pid %d still present during reap; retry", p.PID))
			}
			if already && !force {
				continue
			}
			if !already {
				sent[key] = time.Now()
			}
			err := signalMember(p, force, r.c.RunID)
			logErr = errors.Join(logErr, r.note("process/reap", fmt.Sprintf("pid=%d force=%t result=%v", p.PID, force, err)))

		}
		if active == 0 {
			return errors.Join(logErr, r.note("process/reap", fmt.Sprintf("finished signalled=%d unreadable=%d %s", len(sent), view.Unreadable, view.Warning)))
		}
		time.Sleep(100 * time.Millisecond)
	}
}
