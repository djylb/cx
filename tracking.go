package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
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

func (c *config) mark(cmd *exec.Cmd, dir string) {
	if c.cacheUse != nil {
		inheritCacheLeases(cmd, c.cacheUse.leases)
	}
	// Host defaults (shims, env_file, account settings) come first so a value the
	// job sets explicitly wins: for duplicate keys the last entry is used.
	env := append(cmd.Environ(), c.localEnv...)
	for k, v := range c.Env {
		env = append(env, k+"="+v)
	}
	env = slices.DeleteFunc(env, func(entry string) bool {
		key, _, _ := strings.Cut(entry, "=")
		return strings.EqualFold(key, "CX_CHANGESET") || isSessionEnv(entry)
	})
	if c.Work != nil && c.Work.Changeset != "" {
		env = append(env, "CX_CHANGESET="+c.Work.Changeset)
	}
	host := c.Host
	if host == "" {
		host = "local"
	}
	job := c.Project + "/" + filepath.Base(dir)
	cmd.Env = append(env, "CXRUN_JOB="+job, "CXRUN_RUN="+c.RunID, sessionEnv+"="+host+":"+job)
}
func sameProcess(start, current uint64) bool { return start != 0 && start == current }

func (r *runner) inventory() inventory {
	v := inventory{Processes: []process{}}
	if r.c.RunID == "" {
		v.Warning = "job has no recorded process identity"
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
	if _, err := readJobStatus(dir, &r.s); err != nil {
		return err
	}
	if err := readJobConfig(dir, &r.c); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(r.inventory())
}

// Tree-only matches belong in ps, but cannot authorize a signal.
func (r *runner) reclaim() error {
	if r.c.RunID == "" {
		return nil
	}
	var logErr error
	if r.log == nil {
		f, err := openJobFile(filepath.Join(r.dir, "progress.log"), jobFileAppend)
		logErr = err
		if err == nil {
			defer closeBestEffort(f)
			r.log = f
			defer func() { r.log = nil }()
		}
	}
	note := func(message string) error {
		if r.log == nil {
			return nil
		}
		return r.note("process/reap", message)
	}
	return errors.Join(logErr, r.reapMarked(false, note))
}

func reapManagedProcesses(run string) error {
	r := runner{c: config{RunID: run}}
	return r.reapMarked(true, func(string) error { return nil })
}

func (r *runner) reapMarked(immediate bool, note func(string) error) error {
	var logErr error
	type identity struct {
		pid   int
		start uint64
	}
	// Escalation is driven by when each signal was sent, never by how often
	// the loop runs: a slow inventory scan must not skip the forced signal.
	const termGrace, killGrace = 10 * time.Second, 10 * time.Second
	sent := map[identity]time.Time{}
	killed := map[identity]time.Time{}
	deadline := time.Now().Add(30 * time.Second)
	if err := reclaimTrackedJob(r.c.RunID); err != nil {
		logErr = errors.Join(logErr, err, note(err.Error()))
	}
	for {
		view := r.inventory()
		if view.scanErr != nil {
			return errors.Join(logErr, view.scanErr, note(view.scanErr.Error()))
		}
		active := 0

		for _, p := range view.Processes {
			if !p.Marked {
				continue
			}
			active++
			key := identity{p.PID, p.Start}
			now := time.Now()
			if now.After(deadline.Add(killGrace)) {
				return errors.Join(logErr, fmt.Errorf("pid %d still present during reap; retry", p.PID))
			}
			if killedAt, ok := killed[key]; ok {
				if now.Sub(killedAt) > killGrace {
					return errors.Join(logErr, fmt.Errorf("pid %d still present during reap; retry", p.PID))
				}
				continue
			}
			termAt, already := sent[key]
			force := immediate || runtime.GOOS == "windows" || now.After(deadline) || (already && now.Sub(termAt) >= termGrace)
			if already && !force {
				continue
			}
			if !already {
				sent[key] = now
			}
			if force {
				killed[key] = now
			}
			err := signalMember(p, force, r.c.RunID)
			logErr = errors.Join(logErr, note(fmt.Sprintf("pid=%d force=%t result=%v", p.PID, force, err)))
		}
		if active == 0 {
			return errors.Join(logErr, note(fmt.Sprintf("finished signalled=%d unreadable=%d %s", len(sent), view.Unreadable, view.Warning)))
		}
		time.Sleep(100 * time.Millisecond)
	}
}
