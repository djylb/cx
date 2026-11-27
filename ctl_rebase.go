package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type rebaseRequest struct {
	Text      string
	Session   string
	Child     string `json:",omitempty"`
	Failure   string `json:",omitempty"`
	Submitted bool   `json:",omitempty"`
}

func ctlRebase(ctx context.Context, h endpoint, host, id string, o ctlOptions) error {
	if strings.TrimSpace(o.dir) == "" {
		return errors.New("rebase requires follow-up text")
	}
	if !strings.Contains(id, "/") {
		return errors.New("rebase requires a project job")
	}
	s, target, err := selectResultTurn(ctx, h, id, o.turn, false)
	pending := errors.Is(err, ctlExit(44)) && resultInProgress(s) && target.Number == s.Finished+1
	if err != nil && !pending {
		return err
	}
	if target.RunID == "" {
		return errors.New("rebase requires a registered job generation")
	}
	turn := strconv.FormatInt(target.Number, 10)
	dest, err := resultDest(host, id, target.RunID, turn)
	if err != nil {
		return err
	}
	originPath, err := originFile(host, id, target.RunID)
	if err != nil {
		return err
	}
	var origin submissionOrigin
	if err = readJSON(originPath, &origin); err != nil {
		return fmt.Errorf("read rebase origin: %w", err)
	}
	if origin.Command || origin.Snapshot == "" {
		return errors.New("rebase requires a snapshot agent job")
	}
	slot := dest + ".rebase.json"
	logPath := dest + ".rebase.log"
	lock, err := lockUntil(ctx, rebaseLockPath(dest))
	if err != nil {
		return err
	}
	defer func() { closeBestEffort(lock) }()
	var req rebaseRequest
	err = readJSON(slot, &req)
	replacing := err == nil && req.Child == ""
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	lease, err := lockPath(slot+".worker", os.O_CREATE)
	if err != nil {
		return err
	}
	defer closeBestEffort(lease)
	if req.Child != "" {
		if req.Submitted || lease == nil {
			fmt.Println(req.Child)
			return nil
		}
	} else {
		req.Text, req.Session = o.dir, o.session
		if err = writeJSON(slot, req); err != nil {
			return err
		}
	}
	if !pending && lease != nil {
		closeBestEffort(lock)
		lock = nil
		return executeRebase(ctx, h, host, id, turn, dest, target, false)
	}
	if lease != nil {
		closeBestEffort(lease)
		if err = launchRebaseWorker(host, id, target.RunID, turn, logPath); err != nil {
			req.Failure = err.Error()
			return errors.Join(err, writeJSON(slot, req))
		}
	}
	fmt.Printf("rebase queued for %s turn %s\n", id, turn)
	if replacing {
		fmt.Println("replaced pending request")
	}
	fmt.Printf("controller log %s\n", logPath)
	return nil
}

// A job-wide lock also serializes child name allocation across turns and generations.
func rebaseLockPath(dest string) string {
	return filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(dest))), ".rebase.lock")
}

func launchRebaseWorker(host, id, generation, turn, logPath string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	log, err := openJobFile(logPath, jobFileAppendCreate)
	if err != nil {
		return err
	}
	defer closeBestEffort(log)
	cmd := exec.Command(exe, "node", "rebase-worker", host, id, generation, turn)
	cmd.Stdout, cmd.Stderr = log, log
	if err = startDetachedWorker(cmd); err != nil {
		return err
	}
	go func() { _ = cmd.Wait() }()
	return nil
}

func rebaseWorker(ctx context.Context, args []string) error {
	if len(args) != 4 {
		return errors.New("invalid rebase worker arguments")
	}
	host, id, generation, turn := args[0], args[1], args[2], args[3]
	n, err := strconv.ParseInt(turn, 10, 64)
	if err != nil || n < 0 || strconv.FormatInt(n, 10) != turn || generation == "" {
		return errors.New("invalid rebase worker target")
	}
	dest, err := resultDest(host, id, generation, turn)
	if err != nil {
		return err
	}
	slot := dest + ".rebase.json"
	lock, err := lockUntil(ctx, rebaseLockPath(dest))
	if err != nil {
		return err
	}
	var req rebaseRequest
	err = readJSON(slot, &req)
	if err != nil || req.Submitted {
		closeBestEffort(lock)
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	lease, err := lockPath(slot+".worker", os.O_CREATE)
	closeBestEffort(lock)
	if err != nil || lease == nil {
		return err
	}
	defer closeBestEffort(lease)
	target := observedResult{RunID: generation, Number: n}
	hs, err := loadEndpoints()
	if err != nil {
		return recordRebaseFailure(ctx, dest, err)
	}
	h, err := hs.get(host)
	if err != nil {
		return recordRebaseFailure(ctx, dest, err)
	}
	return executeRebase(ctx, h, host, id, turn, dest, target, true)
}

func executeRebase(ctx context.Context, h endpoint, host, id, turn, dest string, target observedResult, wait bool) error {
	var err error
	if wait {
		err = waitRebaseResult(ctx, h, id, turn, dest, target)
	} else {
		err = fetchResultTurn(ctx, h, id, dest, false, target)
	}
	if err == nil {
		err = dispatchRebase(ctx, h, host, id, dest)
	}
	if err != nil {
		return recordRebaseFailure(ctx, dest, err)
	}
	return nil
}

func recordRebaseFailure(ctx context.Context, dest string, failure error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	lock, err := lockUntil(ctx, rebaseLockPath(dest))
	if err != nil {
		return errors.Join(failure, err)
	}
	defer closeBestEffort(lock)
	slot := dest + ".rebase.json"
	var req rebaseRequest
	if err = readJSON(slot, &req); err != nil {
		return errors.Join(failure, err)
	}
	req.Failure = failure.Error()
	return errors.Join(failure, writeJSON(slot, req))
}

func waitRebaseResult(ctx context.Context, h endpoint, id, turn, dest string, target observedResult) error {
	limit, err := observation("")
	if err != nil {
		return err
	}
	r, err := h.call(ctx, limit, "observe-result", id, target.RunID, turn)
	if err != nil {
		return err
	}
	if r.code != 0 {
		return r.output()
	}
	var s status
	if err = json.Unmarshal(r.data, &s); err != nil {
		return err
	}
	if s.RunID != target.RunID {
		return errJobReplaced
	}
	if !resultTurnCompleted(s, target.Number) {
		return resultUnavailable(fmt.Sprintf("turn %d not sealed (job is %s)", target.Number, s.State))
	}
	if target.Number == s.Finished {
		target.TurnID = s.LastCompleted
	}
	return fetchResultTurn(ctx, h, id, dest, false, target)
}

func dispatchRebase(ctx context.Context, h endpoint, host, id, dest string) error {
	req, err := claimRebase(ctx, h, host, id, dest)
	if err != nil {
		return err
	}
	if !req.Submitted {
		if err = submitRebase(ctx, host, dest, id, req); err != nil {
			return err
		}
		done, err := rebaseSubmitted(host, req.Child)
		if err != nil {
			return err
		}
		if !done {
			return errors.New("rebase child has no submission receipt")
		}
		lock, err := lockUntil(ctx, rebaseLockPath(dest))
		if err != nil {
			return err
		}
		defer closeBestEffort(lock)
		req.Submitted, req.Failure = true, ""
		if err = writeJSON(dest+".rebase.json", req); err != nil {
			return err
		}
	}
	fmt.Println(req.Child)
	return nil
}

func rebaseSubmitted(host, child string) (bool, error) {
	dir, err := submissionDir(host, child)
	if err != nil {
		return false, err
	}
	s, err := readSubmissionProgress(dir)
	if os.IsNotExist(err) {
		return false, nil
	}
	return s.State == "submitted", err
}

func claimRebase(ctx context.Context, h endpoint, host, id, dest string) (rebaseRequest, error) {
	var req rebaseRequest
	lockPath := rebaseLockPath(dest)
	lock, err := lockUntil(ctx, lockPath)
	if err != nil {
		return req, err
	}
	defer closeBestEffort(lock)
	slot := dest + ".rebase.json"
	if err = readJSON(slot, &req); err != nil || req.Child != "" {
		return req, err
	}
	// A claim may precede creation of the submission input. Reserve those
	// names too, including claims from earlier generations of this job.
	root := filepath.Dir(lockPath)
	slots, err := fs.Glob(os.DirFS(root), "*/result/turn-*.rebase.json")
	if err != nil {
		return req, err
	}
	reserved := make(map[string]bool)
	for _, path := range slots {
		var other rebaseRequest
		if err = readJSON(filepath.Join(root, path), &other); err != nil {
			return req, err
		}
		reserved[other.Child] = true
	}
	p, j, _ := strings.Cut(id, "/")
	for n := 1; ; n++ {
		suffix := fmt.Sprintf("-r%d", n)
		child := p + "/" + j[:min(len(j), 96-len(suffix))] + suffix
		if reserved[child] {
			continue
		}
		dir, err := submissionDir(host, child)
		if err != nil {
			return req, err
		}
		if _, err = os.Stat(filepath.Join(dir, "input.json")); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return req, err
		}
		r, err := h.call(ctx, 15*time.Second, "status", "--job", child)
		if err != nil {
			return req, err
		}
		switch r.code {
		case 0:
			continue
		case 11:
			req.Child, req.Failure = child, ""
			return req, writeJSON(slot, req)
		default:
			return req, r.output()
		}
	}
}
