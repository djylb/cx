package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var errDuplicate = errors.New("duplicate submission")

// While the job exists only status.json is authoritative. Clean freezes its last
// snapshot in the receipt before removing the directory.
type receipt struct {
	Archived *status `json:"archived,omitempty"`
	Project  string  `json:"project"`
	Job      string  `json:"job"`
}

func claimKey(root string, c config, s status) error {
	dir := filepath.Join(root, ".dedup")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	path, err := jobPath(dir, c.DedupKey)
	if err != nil {
		return err
	}
	// Reserve the entry before publishing the job. An empty reservation is
	// recoverable: the registry lock excludes other publishers, and no job
	// is registered until a complete receipt has replaced the reservation.
	reservation, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err == nil {
		if err = reservation.Close(); err != nil {
			return err
		}
	} else if !os.IsExist(err) {
		return err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var old receipt
	if len(raw) > 0 {
		if err = json.Unmarshal(raw, &old); err != nil {
			return err
		}

		current := old.Archived
		// A crash before register can leave this name available for a
		// different key. Only a matching job may supersede the receipt.
		previousDir := filepath.Join(root, old.Project, old.Job)
		var previous config
		err = readJSON(filepath.Join(previousDir, "config.json"), &previous)
		if err == nil && previous.DedupKey == c.DedupKey {
			var live status
			if err = readJSON(filepath.Join(previousDir, "status.json"), &live); err != nil {
				return err
			}
			current = &live
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
		if current != nil && (!isTerminal(current.State) || (current.State == "done" && current.CompletedStatus == "completed")) {
			if err = json.NewEncoder(os.Stdout).Encode(current); err != nil {
				return err
			}
			return errDuplicate
		}
	}
	next := receipt{Project: s.Project, Job: s.Job}
	return writeJSON(path, next)
}
func archiveReceipt(root string, c config, s status) error {
	if c.DedupKey == "" {
		return nil
	}
	path := filepath.Join(root, ".dedup", c.DedupKey)
	var rec receipt
	if err := readJSON(path, &rec); err != nil {
		return err
	}
	if rec.Project != s.Project || rec.Job != s.Job {
		return nil
	}
	rec.Archived = &s
	return writeJSON(path, rec)
}
func scanJobs(ctx context.Context, root, project, session string, visit func(status) (bool, error)) error {
	var scan func(string, string) (bool, error)
	scan = func(dir, p string) (bool, error) {
		entries, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
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
			statePath := filepath.Join(path, "status.json")
			fi, err := os.Stat(statePath)
			if os.IsNotExist(err) || (err == nil && fi.IsDir()) {
				if p == "" {
					if stop, scanErr := scan(path, entry.Name()); stop || scanErr != nil {
						return stop, scanErr
					}
				}
				continue
			}
			if err != nil {
				return false, err
			}
			if project != "" && project != p {
				continue
			}
			var s status
			if err = projected(ctx, path, &s); os.IsNotExist(err) {
				continue
			} else if err != nil {
				return false, err
			}
			if session == "" || session == s.Session {
				if stop, err := visit(s); stop || err != nil {
					return stop, err
				}
			}
		}
		return false, nil
	}
	_, err := scan(root, "")
	return err
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
