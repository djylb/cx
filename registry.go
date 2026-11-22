package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var errDuplicate = errors.New("duplicate submission")

const nodeStateFormat = 1
const nodeStateFormatFile = ".state-format"

var errStateFormat = errors.New("unsupported node state format; select a separate state root")

func checkNodeStateFormat(root string) (bool, error) {
	var version int
	path := filepath.Join(root, nodeStateFormatFile)
	if err := readJSON(path, &version); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, fmt.Errorf("%w: %s: %w", errStateFormat, path, err)
	}
	if version != nodeStateFormat {
		return false, fmt.Errorf("%w: %s has version %d", errStateFormat, path, version)
	}
	return true, nil
}

// The first registration or restart checks unmarked roots from the
// state-log rollout. The caller holds the registry lock.
func ensureNodeStateFormat(ctx context.Context, root string) error {
	if known, err := checkNodeStateFormat(root); err != nil || known {
		return err
	}
	err := walkJobDirs(ctx, root, "", func(dir, project, name string) (bool, bool, error) {
		if project == "" && strings.EqualFold(name, "cache") {
			return true, false, nil
		}
		for _, old := range []string{"status.json", "config.json"} {
			if _, err := os.Lstat(filepath.Join(dir, old)); err == nil {
				return false, false, fmt.Errorf("%w: %s contains %s", errStateFormat, dir, old)
			} else if !os.IsNotExist(err) {
				return false, false, err
			}
		}
		current, _, err := latestState(dir)
		if err != nil || current.Seq == 0 {
			return false, false, err
		}
		record, _, err := latestStateKind(dir, outboxSeenKind)
		if err == nil && record.Seq != 0 {
			var at stateCursor
			if decodeErr := json.Unmarshal(record.Value, &at); decodeErr != nil || at.Seg == 0 || at.Seq == 0 || at.Off == 0 {
				return true, false, fmt.Errorf("%w: %s has an incompatible outbox checkpoint", errStateFormat, dir)
			}
		}
		return true, false, err
	})
	if err != nil {
		return err
	}
	data := fmt.Appendf(nil, "%d\n", nodeStateFormat)
	return publishImmutable(filepath.Join(root, nodeStateFormatFile), data, root)
}

type receipt struct {
	Project string `json:"project"`
	Job     string `json:"job"`
	RunID   string `json:"run_id"`
}

func dedupReceipt(root, key string) (string, receipt, error) {
	var ref receipt
	dir, err := jobPath(filepath.Join(root, ".dedup"), key)
	if err != nil {
		return "", ref, err
	}
	record, _, err := latestStateKind(dir, "claim")
	if err == nil && record.Seq != 0 {
		err = json.Unmarshal(record.Value, &ref)
	}
	return dir, ref, err
}

// The registry lock serializes claim publication and archive creation.
func claimKey(root string, c config, s status) error {
	dir, old, err := dedupReceipt(root, c.DedupKey)
	if err != nil {
		return err
	}
	if old.RunID != "" {
		current, _, statusErr := loadJobStatus(filepath.Join(root, old.Project, old.Job))
		if statusErr != nil && !errors.Is(statusErr, errNotRegistered) {
			return statusErr
		}
		if current.RunID != old.RunID {
			current, statusErr = archivedDedupStatus(dir, old)
			if statusErr != nil {
				return statusErr
			}
		}
		if current.State != "" && (!current.final || (current.State == "done" && current.CompletedStatus == "completed")) {
			if outputErr := json.NewEncoder(os.Stdout).Encode(current); outputErr != nil {
				return outputErr
			}
			return errDuplicate
		}
	}
	writer, err := openLogWriter(dir, true)
	if err != nil {
		return err
	}
	next := receipt{Project: s.Project, Job: s.Job, RunID: c.RunID}
	err = writer.Append("claim", next)
	if err == nil {
		err = writer.sync()
	}
	if retireErr := errors.Join(err, writer.Close()); retireErr != nil {
		return retireErr
	}
	if err := syncJobTree(dir, root); err != nil {
		return err
	}
	// The new claim supersedes every older claim once its segment is durable.
	if segments, err := logSegments(writer.dir); err == nil {
		for _, seg := range segments {
			if seg != writer.seg {
				removeJobSegment(segmentPath(writer.dir, seg))
			}
		}
	}
	return nil
}

func archivedDedupStatus(dir string, ref receipt) (status, error) {
	var s status
	archive := filepath.Join(dir, "final", ref.RunID)
	entries, err := os.ReadDir(archive)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	for i := len(entries) - 1; i >= 0; i-- {
		record, _, err := latestFileKind(filepath.Join(archive, entries[i].Name()), "status")
		if err != nil {
			return s, err
		}
		if record.Final {
			err = decodeJobStatus(record, &s)
			return s, err
		}
	}
	return s, nil
}

// archiveAndDetachJob runs under the registry lock. It releases the owner
// handle before renaming the directory, while still holding the mailbox locks.
func archiveAndDetachJob(ctx context.Context, root, dir string, c config, s status, syncArchive func(string, string) error) error {
	owner, err := openJobOwner(ctx, dir, jobFileLockExisting)
	if err != nil {
		return err
	}
	if owner == nil {
		return errJobOwnerBusy
	}
	unlock, err := lockJobMailboxes(ctx, dir)
	if err != nil {
		return errors.Join(err, owner.Close())
	}
	defer unlock()
	if err := errors.Join(archiveReceipt(root, c, s, syncArchive), owner.Close()); err != nil {
		return err
	}
	return detachCleanJob(root, dir, s)
}

func archiveReceipt(root string, c config, s status, syncArchive func(string, string) error) error {
	if c.DedupKey == "" || s.State != "done" || s.CompletedStatus != "completed" {
		return nil
	}
	dir, ref, err := dedupReceipt(root, c.DedupKey)
	if err != nil || ref.RunID != s.RunID {
		return err
	}
	job := filepath.Join(root, s.Project, s.Job)
	record, cursor, err := latestState(job)
	if err != nil {
		return err
	}
	if !record.Final {
		return errors.New("cannot archive an unsealed job")
	}
	// Link the flushed segment before detaching the job. A matching live
	// run still takes precedence if clean fails after making the link.
	archive := filepath.Join(dir, "final", ref.RunID)
	if err = os.MkdirAll(archive, 0700); err != nil {
		return err
	}
	final := filepath.Join(archive, fmt.Sprintf("%020d.jsonl", cursor.Seq))
	err = os.Link(segmentPath(filepath.Join(job, "state"), cursor.Seg), final)
	if err != nil && !os.IsExist(err) {
		return err
	}
	return syncArchive(archive, root)
}

// The root may contain flat jobs and project directories. A top-level
// directory without a published job status may be a project.
func walkJobDirs(ctx context.Context, root, project string, visit func(dir, project, job string) (registered, stop bool, err error)) error {
	var scan func(string, string) (bool, error)
	scan = func(dir, p string) (bool, error) {
		entries, err := os.ReadDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
				continue
			}
			if p == "" && project != "" && entry.Name() != project {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			registered, stop, err := visit(path, p, entry.Name())
			if stop || err != nil {
				return stop, err
			}
			if p == "" && !registered {
				if stop, scanErr := scan(path, entry.Name()); stop || scanErr != nil {
					return stop, scanErr
				}
			}
		}
		return false, nil
	}
	_, err := scan(root, "")
	return err
}

func scanJobs(ctx context.Context, root, project, session string, visit func(status) (bool, error)) error {
	return walkJobDirs(ctx, root, project, func(path, p, _ string) (bool, bool, error) {
		id, s, _, err := captureJobIdentity(path)
		if errors.Is(err, os.ErrNotExist) {
			return false, false, nil
		}
		if err != nil || (project != "" && p != project) || (session != "" && session != s.Session) {
			return true, false, err
		}
		if _, err = projectJobStatus(ctx, path, &s, id); errors.Is(err, os.ErrNotExist) {
			return false, false, nil
		} else if err != nil {
			return true, false, err
		}
		stop, err := visit(s)
		return true, stop, err
	})
}

func listJobs(ctx context.Context, root, project, session string, briefOnly bool) error {
	out := json.NewEncoder(os.Stdout)
	return scanJobs(ctx, root, project, session, func(s status) (bool, error) {
		if briefOnly {
			return false, out.Encode(briefStatus(s))
		}
		return false, out.Encode(struct {
			RateLimit rateLimitStatus `json:"rate_limit"`
			status
		}{RateLimit: s.rateLimitStatus, status: s})
	})
}

func briefStatus(s status) any {
	return struct {
		LastEvent time.Time       `json:"last_event_time"`
		RateLimit rateLimitStatus `json:"rate_limit"`
		Project   string          `json:"project"`
		Job       string          `json:"job"`
		Session   string          `json:"session"`
		Profile   string          `json:"profile,omitempty"`
		State     string          `json:"status"`
		Turn      string          `json:"turn_id"`
		turnSummary
		Finished int64 `json:"completed_turns"`
	}{
		Project: s.Project, Job: s.Job, Session: s.Session, Profile: s.Profile, State: s.State,
		Turn: s.Turn, Finished: s.Finished, LastEvent: s.LastEvent,
		turnSummary: s.turnSummary, RateLimit: s.rateLimitStatus,
	}
}
