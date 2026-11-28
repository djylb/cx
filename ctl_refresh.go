package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type refreshRequest struct {
	Work     *moduleWork        `json:"work,omitempty"`
	Snapshot SnapshotID         `json:"snapshot"`
	Source   string             `json:"source"`
	Sources  map[string]string  `json:"sources,omitempty"`
	Branches map[string]*string `json:"branches,omitempty"`
}

const refreshRequestPrefix = "refresh-"

type refreshTodo struct {
	refreshRequest
	ID        string
	Submitted bool
	Failure   string
}

type jobBaseline struct {
	refreshRequest
	FromTurn int64     `json:"from_turn"`
	Time     time.Time `json:"time"`
	Request  string    `json:"request,omitempty"`
	Question string    `json:"question,omitempty"`
}

func ctlRefresh(ctx context.Context, hs endpoints, h endpoint, host, id string, o ctlOptions) error {
	project, _, ok := strings.Cut(id, "/")
	if !ok {
		return errors.New("refresh requires a project job")
	}
	statusReply, err := h.call(ctx, 15*time.Second, "status", "--job", id)
	if err != nil {
		return err
	}
	if statusReply.code != 0 {
		return statusReply.output()
	}
	var s status
	if err = json.Unmarshal(statusReply.data, &s); err != nil {
		return err
	}
	if _, err = liveAgentEngine(s); err != nil {
		return err
	}
	if s.RunID == "" {
		return errors.New("refresh requires a registered job generation")
	}
	turn := s.Finished
	if resultInProgress(s) {
		turn++
	}
	path, err := originFile(host, id, s.RunID)
	if err != nil {
		return err
	}
	var origin submissionOrigin
	if err = readJSON(path, &origin); err != nil {
		return err
	}
	if origin.Command || origin.Snapshot == "" {
		return errors.New("refresh requires a snapshot agent job")
	}
	src := o.dir
	if src == "" {
		src = origin.Source
	}
	src, err = filepath.Abs(src)
	if err != nil {
		return err
	}
	src, err = filepath.EvalSymlinks(src)
	if err != nil {
		return err
	}
	_, def, err := resolveProject(ctx, src, project)
	if err != nil {
		return err
	}
	sid, err := submitSnapshot(ctx, src, ctlOptions{history: origin.History}, def)
	if err != nil {
		return err
	}
	dest, err := resultDest(host, id, s.RunID, strconv.FormatInt(turn, 10))
	if err != nil {
		return err
	}
	lock, err := lockUntil(ctx, rebaseLockPath(dest))
	if err != nil {
		return err
	}
	defer closeBestEffort(lock)
	slot := dest + ".rebase.json"
	var req rebaseRequest
	if err = readJSON(slot, &req); err != nil && !os.IsNotExist(err) {
		return err
	}
	submission := jobSubmission{Snapshot: sid}
	if err = verifySourceBase(ctx, src, &submission, ctlOptions{}); err != nil {
		return err
	}
	refresh := refreshRequest{Snapshot: sid, Source: src, Sources: submission.Sources, Branches: submission.Branches}
	var oldWork *moduleWork
	if len(origin.ModuleOptions) != 0 {
		info, infoErr := getJobClaimInfo(ctx, h, id)
		if infoErr != nil {
			return infoErr
		}
		if info.RunID != s.RunID {
			return errJobReplaced
		}
		oldWork = info.Work
		if oldWork != nil && len(oldWork.Modules) != 0 {
			opts := ctlOptions{modules: oldWork.Modules, sandbox: "workspace-write", moduleOverride: o.moduleOverride,
				accept: oldWork.AcceptScripts, noAccept: oldWork.NoAccept, changeset: oldWork.Changeset}
			if oldWork.ReadOnly {
				opts.sandbox = "read-only"
			}
			if oldWork.AcceptTimeout != 0 {
				opts.acceptTimeout = oldWork.AcceptTimeout.String()
			}
			submission.ID, submission.Host, submission.FoldPaths = id, host, oldWork.FoldPaths || foldTargetPaths(h.Platform)
			if err = prepareModuleWork(ctx, &submission, def, opts); err != nil {
				return err
			}
			refresh.Work = &submission.moduleWork
		}
	}
	if req.Refresh == nil || req.Refresh.Submitted || req.Refresh.Snapshot != sid || req.Refresh.Source != src ||
		!reflect.DeepEqual(req.Refresh.Work, refresh.Work) {
		req.Refresh = &refreshTodo{refreshRequest: refresh, ID: refreshRequestPrefix + rand.Text()}
	}
	if err = writeJSON(slot, req); err != nil {
		return err
	}
	if err = Publish(ctx, h, sid, nil); err == nil {
		if refresh.Work != nil && !reflect.DeepEqual(oldWork, refresh.Work) {
			if _, err = checkModuleClaims(ctx, hs, claimAddress{Host: host, Job: id}, nil, *refresh.Work); err != nil {
				return err
			}
		}
		data, encodeErr := json.Marshal(req.Refresh.refreshRequest)
		err = encodeErr
		if err == nil {
			var response reply
			response, err = h.call(ctx, 30*time.Second, "refresh-job", id, s.RunID, req.Refresh.ID, string(data))
			if err == nil {
				err = response.output()

			}
		}
	}
	req.Refresh.Submitted = err == nil
	req.Refresh.Failure = ""
	if err != nil {
		req.Refresh.Failure = err.Error()
	}
	err = errors.Join(err, writeJSON(slot, req))
	if err == nil {
		fmt.Printf("refresh queued for %s (%s)\n", id, string(sid)[:12])
	}
	return err
}

func readJobBaseline(work string) (jobBaseline, error) {
	var base jobBaseline
	err := readJSON(filepath.Join(work, "input", "baseline.json"), &base)
	if os.IsNotExist(err) {
		return jobBaseline{}, nil
	}
	return base, err
}

func baselineColumn(row fields) any {
	id, _ := row["baseline"].(string)
	if len(id) < 12 {
		return nil
	}
	return fmt.Sprintf("%s %v", id[:12], row["baseline_at"])
}
