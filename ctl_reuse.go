package main

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

type reuseQuery struct {
	Env                                   map[string]string
	Profile                               string `json:",omitempty"`
	Host, ID, Key, Dir, ScriptHash, Shell string
	Argv                                  []string
}
type reuseHit struct {
	Job    string `json:"job"`
	Host   string `json:"host"`
	Log    string `json:"log"`
	Exit   int64  `json:"exit_code"`
	Reused bool   `json:"reused"`
}

func lookupReuse(ctx context.Context, h endpoint, q reuseQuery) (bool, error) {
	data, e := json.Marshal(q)
	if e != nil {
		return false, e
	}
	r, e := h.call(ctx, 30*time.Second, "find-reuse", string(data))
	if e != nil {
		return false, e
	}
	if r.code != 0 {
		return false, r.output()
	}
	if len(r.data) == 0 {
		return false, nil
	}
	var hit reuseHit
	if e = json.Unmarshal(r.data, &hit); e != nil {
		return false, e
	}
	if e = r.output(); e != nil {
		return false, e
	}
	if hit.Exit != 0 {
		return true, ctlExit(hit.Exit)
	}
	return true, nil
}
func findReuse(ctx context.Context, raw string) error {
	var q reuseQuery
	if e := json.Unmarshal([]byte(raw), &q); e != nil {
		return e
	}
	root, e := stateRoot()
	if e != nil {
		return e
	}
	dir, e := jobDir(root, q.ID)
	if e != nil {
		return e
	}
	// When the requested tag exists, only that job may satisfy the request.
	_, e = os.Stat(filepath.Join(dir, "config.json"))
	exists := e == nil
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	p, _, ok := strings.Cut(q.ID, "/")
	if !ok {
		return errors.New("expected qualified job")
	}
	if !exists && q.Dir == "" && q.Key == "" {
		return nil
	}
	profile, _, e := selectedProfile(root, q.Profile, true)
	if e != nil {
		return e
	}
	var hit *reuseHit
	var newest time.Time
	e = scanJobs(ctx, root, p, "", func(s status) (bool, error) {
		if s.Profile != profile || s.Engine != "cmd" || s.ExitCode == nil || s.State != "done" && s.State != "failed" || s.LastEvent.Before(newest) {
			return false, nil
		}
		id := s.Project + "/" + s.Job
		if exists && id != q.ID {
			return false, nil
		}
		path, sourceErr := jobDir(root, id)
		if sourceErr != nil {
			return false, sourceErr
		}
		var work string
		sourceErr = readJSON(filepath.Join(path, "cx-work.json"), &work)
		if sourceErr == nil {
			if q.Key == "" {
				return false, nil
			}
			var req jobSubmission
			if sourceErr = readJSON(filepath.Join(work, "request.json"), &req); sourceErr != nil {
				return false, sourceErr
			}
			if req.ReuseKey != q.Key {
				return false, nil
			}
		} else if !os.IsNotExist(sourceErr) {
			return false, sourceErr
		} else {
			if q.Key != "" {
				return false, nil
			}
			var c config
			if sourceErr = readJSON(filepath.Join(path, "config.json"), &c); sourceErr != nil {
				return false, sourceErr
			}
			targetDir := q.Dir
			if targetDir == "" {
				targetDir = filepath.Join(root, ".work", id)
			}
			cwd, absErr := filepath.Abs(targetDir)
			if absErr != nil {
				return false, absErr
			}
			if c.Dir != cwd || !maps.Equal(c.Env, q.Env) {
				return false, nil
			}
			argv := q.Argv
			if q.ScriptHash != "" {
				sh := scriptShell(q.Shell)
				ext, scriptErr := scriptExtension(sh)
				if scriptErr != nil {
					return false, scriptErr
				}
				argv = scriptArgv(sh, filepath.Join(root, ".scripts", id, q.ScriptHash+ext))
			}
			if !slices.Equal(argv, c.Argv) {
				return false, nil
			}
		}
		newest = s.LastEvent
		hit = &reuseHit{Job: id, Reused: true, Exit: *s.ExitCode, Host: q.Host, Log: filepath.Join(path, "output.log")}
		return false, nil
	})
	if e != nil {
		return e
	}
	if hit != nil {
		return json.NewEncoder(os.Stdout).Encode(hit)
	}
	return nil
}
func runReuseKey(sid SnapshotID, src string, cmd []string) string {
	b, _ := json.Marshal([]any{sid, src, cmd})
	return snapSum(b)
}

var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func addCommandEnv(env *map[string]string, entry string) error {
	key, val, ok := strings.Cut(entry, "=")
	if !ok || !envName.MatchString(key) {
		return errors.New("--env requires NAME=VALUE")
	}
	if *env == nil {
		*env = map[string]string{}
	}
	(*env)[key] = val
	return nil
}
