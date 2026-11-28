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

	"github.com/djylb/timeshim"
)

type queuedJob struct {
	ClaimSource    *claimAddress `json:"claim_source,omitempty"`
	OverrideReason *string       `json:"-"`
	Rebase         string        `json:",omitempty"`
	Request        jobSubmission
	Origin         submissionOrigin
}
type submissionProgress struct {
	OverrideReason  *string  `json:"override_reason,omitempty"`
	ClaimsUnchecked []string `json:"claims_unchecked,omitempty"`
	moduleListing
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
	home, e := controllerCacheRoot()
	if e != nil {
		return "", e
	}
	return jobPath(filepath.Join(home, "submissions"), host)
}

func submissionDir(host, id string) (string, error) {
	root, e := submissionRoot(host)
	if e != nil {
		return "", e
	}
	return jobDir(root, id)
}
func deployJob(ctx context.Context, h endpoint, q queuedJob, report func(string, int64), registered func(string)) error {
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
	if h.SSH != "" || q.Rebase != "" {
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
		return errors.Join(ctlExit(9), e)
	}
	if ack.RunID == "" {
		return errors.Join(ctlExit(9), errors.New("node registration receipt has no run ID"))
	}
	registered(ack.RunID)
	origin, e := originFile(req.Host, req.ID, ack.RunID)
	if e != nil {
		return e
	}
	return writeJSON(origin, q.Origin)
}
func queueJob(ctx context.Context, hs endpoints, q queuedJob, sync, force bool, unchecked *[]string) error {
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
	worker, e := lockPath(filepath.Join(dir, "worker.lock"), os.O_CREATE)
	if e != nil {
		return e
	}
	var failed bool
	if worker != nil {
		defer func() {
			if worker != nil {
				closeBestEffort(worker)
			}
		}()
		s, progressErr := readInactiveSubmissionProgress(dir)
		if progressErr != nil && !os.IsNotExist(progressErr) {
			return progressErr
		}
		if s.State == "submitted" && req.writableClaim() {
			if unchecked != nil {
				*unchecked = s.ClaimsUnchecked
			}
			reportUncheckedClaims(s.ClaimsUnchecked)
			return snapImmutable(input, data)
		}
		failed = s.State == "submit_failed"
	}
	if failed && !force {
		return errors.New("previous submission failed; use --force to resubmit")
	}
	work := req.moduleWork
	if q.OverrideReason != nil {
		work.ModuleOverride = *q.OverrideReason
	}
	unverified, e := checkModuleClaims(ctx, hs, claimAddress{Host: req.Host, Job: req.ID}, q.ClaimSource, work)
	if e != nil {
		return e
	}
	if unchecked != nil {
		*unchecked = unverified
	}
	if failed {
		e = atomicWrite(input, data)
	} else {
		e = snapImmutable(input, data)
	}
	if e != nil {
		return e
	}
	if q.OverrideReason != nil && *q.OverrideReason != "" {
		log, err := openJobFile(filepath.Join(dir, "submit.log"), jobFileAppendCreate)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(log, "rebase retry override: %q\n", *q.OverrideReason)
		if err = errors.Join(err, log.Close()); err != nil {
			return err
		}
	}
	if e = rememberRoute(req.ID, req.Host); e != nil {
		return e
	}
	if worker != nil {
		if req.writableClaim() {
			p, j, _ := strings.Cut(req.ID, "/")
			queued := submissionProgress{moduleListing: req.listing(), State: "submit_pending", Host: req.Host, Project: p, Job: j,
				Session: req.Session, Role: req.Role, ClaimsUnchecked: unverified, OverrideReason: q.OverrideReason, Reason: "awaiting submission worker", Log: filepath.Join(dir, "submit.log")}
			if e = writeJSON(filepath.Join(dir, "submission.json"), queued); e != nil {
				return e
			}
		}
		closeBestEffort(worker)
		worker = nil
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
		case <-timeshim.After(timeshim.Or(contextClock(ctx)), 5*time.Second):
			return errors.New("submission worker acknowledgement timed out")
		}
	}
	if sync {
		_, e = observeSubmission(ctx, req.Host, req.ID, "wait", transferLimit)
		if e == nil && unchecked != nil {
			progress, readErr := readSubmissionProgress(dir)
			if readErr != nil {
				return readErr
			}
			*unchecked = progress.ClaimsUnchecked
		}
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
	prior, progressErr := readInactiveSubmissionProgress(dir)
	if progressErr != nil && !os.IsNotExist(progressErr) {
		return progressErr
	}
	if prior.State == "submitted" {
		_, _ = fmt.Fprintln(os.Stdout, "ready")
		return nil
	}
	s := submissionProgress{ClaimsUnchecked: prior.ClaimsUnchecked, OverrideReason: prior.OverrideReason, moduleListing: q.Request.listing(), State: "submitting", Host: host, Job: j, Project: p, Session: q.Request.Session, Role: q.Request.Role, Reason: "waiting for executor registration", PID: os.Getpid(), Log: filepath.Join(dir, "submit.log")}
	path := filepath.Join(dir, "submission.json")
	if e = writeJSON(path, s); e != nil {
		return e
	}
	_, _ = fmt.Fprintln(os.Stdout, "ready")
	os.Stdout = os.Stderr // Parent closes the acknowledgement pipe after this line.
	if q.Request.ModuleOverride != "" {
		diagnosticf("cx: module override for %s (%q)\n", strings.Join(q.Request.Modules, ", "), q.Request.ModuleOverride)
	}
	var runID string
	defer func() {
		code := 0
		uncertain := runID == "" && (errors.Is(err, ctlExit(9)) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled))
		if err != nil {
			code = 1
			if exit, ok := errors.AsType[ctlExit](err); ok {
				code = int(exit)
			}
			s.State = "submit_failed"
			if uncertain {
				s.State = "submit_unverified"
			}
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
	// The claim check ran once in queueJob, before the worker started.
	return deployJob(ctx, h, q, func(phase string, n int64) { s.Phase = phase; s.Bytes = n; _ = writeJSON(path, s) }, func(run string) { runID = run })
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
	defer closeBestEffort(l)
	return readInactiveSubmissionProgress(dir)
}

// The caller owns worker.lock, so a submitting record is from a stopped worker.
func readInactiveSubmissionProgress(dir string) (submissionProgress, error) {
	var s submissionProgress
	err := readJSON(filepath.Join(dir, "submission.json"), &s)
	if err == nil && s.State == "submitting" {
		s.State = "submit_failed"
		if len(s.ModuleClaims) != 0 {
			s.State = "submit_unverified"
		}
		s.Reason = "submission worker stopped before confirming registration"
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
		if s.State == "submit_failed" || s.State == "submit_unverified" {
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
