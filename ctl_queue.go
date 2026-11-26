package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type queuedJob struct {
	Request jobSubmission
	Origin  submissionOrigin
}
type submissionProgress struct {
	Code    *int   `json:"rc"`
	Phase   string `json:"phase,omitempty"`
	State   string `json:"status"`
	Job     string `json:"job"`
	Project string `json:"project"`
	Session string `json:"session"`
	Role    string `json:"role"`
	Host    string `json:"host"`
	Reason  string `json:"reason"`
	Log     string `json:"log"`
	Bytes   int64  `json:"uploaded_bytes,omitempty"`
	PID     int    `json:"pid"`
}

func submissionRoot(host string) (string, error) {
	home, e := os.UserHomeDir()
	if e != nil {
		return "", e
	}
	return jobPath(filepath.Join(home, ".cache", "cx", "submissions"), host)
}

func submissionDir(host, id string) (string, error) {
	root, e := submissionRoot(host)
	if e != nil {
		return "", e
	}
	return jobDir(root, id)
}
func deployJob(ctx context.Context, h endpoint, q queuedJob, report func(string, int64)) error {
	req := q.Request
	if req.Snapshot != "" {
		if e := Publish(ctx, h, req.Snapshot, func(n int64) { report("snapshot", n) }); e != nil {
			return e
		}
		if len(req.Branches) > 0 {
			m, e := loadSnapshot(req.Snapshot)
			if e != nil {
				return e
			}
			refs := map[string]string{}
			for _, r := range m.Repositories {
				if branch := req.Branches[r.Path]; r.History && branch != nil {
					refs[r.Path] = "refs/heads/" + *branch
				}
			}
			if len(refs) > 0 {
				if e = publishMirrorRefs(ctx, h, req.Snapshot, refs); e != nil {
					return e
				}
			}
		}
	}
	data, e := json.Marshal(req)
	if e != nil {
		return e
	}
	var r reply
	if h.SSH != "" {
		if e = sendTask(ctx, h, req, data, func(n int64) { report("packet", n) }); e != nil {
			return e
		}
		e = retryTransfer(ctx, func() error {
			var err error
			r, err = h.invoke(ctx, 2*time.Minute, false, "start-installed-task", req.ID, snapSum(data))
			return err
		})
	} else {
		r, e = h.call(ctx, 2*time.Minute, "start-job", string(data))
	}
	if e != nil {
		return e
	}
	if r.code != 0 {
		return r.output()
	}
	var ack status
	if e = json.Unmarshal(r.data, &ack); e != nil {
		return e
	}
	origin, e := originFile(req.Host, req.ID, ack.RunID)
	if e != nil {
		return e
	}
	return writeJSON(origin, q.Origin)
}
func queueJob(ctx context.Context, q queuedJob, sync bool) error {
	req := q.Request
	dir, e := submissionDir(req.Host, req.ID)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	l, e := lockUntil(ctx, filepath.Join(dir, "schedule.lock"))
	if e != nil {
		return e
	}
	defer closeBestEffort(l)
	input := filepath.Join(dir, "input.json")
	data, e := json.Marshal(q)
	if e != nil {
		return e
	}
	if e = snapImmutable(input, data); e != nil {
		return e
	}
	if e = rememberRoute(req.ID, req.Host); e != nil {
		return e
	}
	worker, e := lockPath(filepath.Join(dir, "worker.lock"), os.O_CREATE)
	if e != nil {
		return e
	}
	if worker != nil {
		_ = worker.Close()
		exe, launchErr := os.Executable()
		if launchErr != nil {
			return launchErr
		}
		logPath := filepath.Join(dir, "submit.log")
		log, launchErr := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if launchErr != nil {
			return launchErr
		}
		defer closeBestEffort(log)
		cmd := exec.Command(exe, "node", "submit-worker", req.Host, req.ID)
		cmd.Stderr = log
		rd, wr, launchErr := os.Pipe()
		if launchErr != nil {
			return launchErr
		}
		defer closeBestEffort(rd)
		cmd.Stdout = wr
		if launchErr = startDetachedWorker(cmd); launchErr != nil {
			_ = wr.Close()
			return launchErr
		}
		_ = wr.Close()
		go func() { _ = cmd.Wait() }()
		ack := make(chan error, 1)
		go func() {
			line, ackErr := bufio.NewReader(rd).ReadString('\n')
			if ackErr == nil && line != "ready\n" {
				ackErr = errors.New("unexpected submission acknowledgement")
			}
			ack <- ackErr
		}()
		select {
		case launchErr = <-ack:
			if launchErr != nil {
				return launchErr
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return errors.New("submission worker acknowledgement timed out")
		}
	}
	if sync {
		_, e = observeSubmission(ctx, req.Host, req.ID, "wait", transferLimit)
		return e
	}
	return nil
}
func submissionWorker(ctx context.Context, host, id string) (err error) {
	dir, e := submissionDir(host, id)
	if e != nil {
		return e
	}
	l, e := lockUntil(ctx, filepath.Join(dir, "worker.lock"))
	if e != nil {
		return e
	}
	defer closeBestEffort(l)
	var q queuedJob
	if e = readJSON(filepath.Join(dir, "input.json"), &q); e != nil {
		return e
	}
	p, j, _ := strings.Cut(id, "/")
	s := submissionProgress{State: "submitting", Host: host, Job: j, Project: p, Session: q.Request.Session, Role: q.Request.Role, Reason: "waiting for executor registration", PID: os.Getpid(), Log: filepath.Join(dir, "submit.log")}
	path := filepath.Join(dir, "submission.json")
	if e = writeJSON(path, s); e != nil {
		return e
	}
	if _, e = fmt.Fprintln(os.Stdout, "ready"); e != nil {
		return e
	}
	os.Stdout = os.Stderr // Parent closes the acknowledgement pipe after this line.
	defer func() {
		code := 0
		if err != nil {
			code = 1
			var exit ctlExit
			if errors.As(err, &exit) {
				code = int(exit)
			}
			s.State = "submit_failed"
			s.Reason = err.Error()
		} else {
			s.State = "submitted"
			s.Reason = ""
		}
		s.Code = &code
		err = errors.Join(err, writeJSON(path, s))
	}()
	hs, e := loadEndpoints()
	if e != nil {
		return e
	}
	h, e := hs.get(host)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, transferLimit)
	defer cancel()
	if h.SSH != "" {
		if e = deploy(ctx, hs, host); e != nil {
			return e
		}
		h = hs[host]
	}
	return deployJob(ctx, h, q, func(phase string, n int64) { s.Phase = phase; s.Bytes = n; _ = writeJSON(path, s) })
}
func readSubmissionProgress(dir string) (submissionProgress, error) {
	var s submissionProgress
	path := filepath.Join(dir, "submission.json")
	if err := readJSON(path, &s); err != nil || s.State != "submitting" {
		return s, err
	}
	l, err := lockPath(filepath.Join(dir, "worker.lock"), os.O_CREATE)
	if err != nil || l == nil {
		return s, err
	}
	_ = l.Close()
	if err = readJSON(path, &s); err == nil && s.State == "submitting" {
		s.State = "submit_failed"
		s.Reason = "submission worker stopped before registration"
		code := 125
		s.Code = &code
	}
	return s, err
}

func observeSubmission(ctx context.Context, host, id, op string, limit time.Duration) (bool, error) {
	dir, e := submissionDir(host, id)
	if e != nil {
		return false, e
	}
	end := time.Now().Add(limit)
	for {
		s, e := readSubmissionProgress(dir)
		if os.IsNotExist(e) {
			return false, nil
		}
		if e != nil {
			return false, e
		}
		if s.State == "submitted" {
			return false, nil
		}
		if op == "status" || op == "show" {
			return true, json.NewEncoder(os.Stdout).Encode(s)
		}
		if s.State == "submit_failed" {
			diagnosticf("%v\n", s.Reason)
			return true, ctlExit(8)
		}
		if op != "tail" && op != "wait" && op != "adopt" {
			return true, errors.New("job is being submitted")
		}
		if time.Now().After(end) {
			return true, ctlExit(124)
		}
		if e = pause(ctx, min(150*time.Millisecond, time.Until(end))); e != nil {
			return true, e
		}
	}
}
