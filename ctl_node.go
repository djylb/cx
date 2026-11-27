package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/djylb/timeshim"
)

func hostSettings(ctx context.Context, root string, sets []string, init bool) error {
	if len(sets) == 0 && !init {
		var raw json.RawMessage
		err := readJSON(filepath.Join(root, hostConfigName), &raw)
		if err == nil {
			return json.NewEncoder(os.Stdout).Encode(raw)
		}
		if !os.IsNotExist(err) {
			return err
		}
		return run(ctx, []string{"host", "--state", root})
	}
	if e := os.MkdirAll(root, 0700); e != nil {
		return e
	}
	lock, e := lockUntil(ctx, filepath.Join(root, ".host-config.lock"))
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	dst := filepath.Join(root, hostConfigName)
	h, e := readHostConfig(root)
	if e != nil {
		return e
	}
	if init {
		if _, e = os.Stat(dst); e == nil {
			return nil
		} else if !os.IsNotExist(e) {
			return e
		}
	}
	// Preserve extension fields and explicit slot overrides in existing files.
	var obj fields
	b, e := readSharedFile(dst)
	if os.IsNotExist(e) {
		obj = fields{"load_cmd": h.LoadCmd, "slots": h.Slots}
	} else if e != nil {
		return e
	} else if e = decodeRows(b, &obj); e != nil {
		return e
	}
	slots := map[string]int{}
	b, e = json.Marshal(obj["slots"])
	if e != nil {
		return e
	}
	if obj["slots"] != nil {
		if e = json.Unmarshal(b, &slots); e != nil {
			return e
		}
	}
	for _, s := range sets {
		k, v, ok := strings.Cut(s, "=")
		if !ok {
			return errors.New("expected K=V")
		}
		switch {
		case k == "load_cmd" || k == "psmux" || k == "mux_socket" || k == "mux_session" || k == "env_file" || k == "default_profile":
			obj[k] = v
		case k == "cache_max_bytes" || k == "min_free_bytes":
			n, parseErr := strconv.ParseUint(v, 10, 64)
			if parseErr != nil {
				return fmt.Errorf("invalid %s: %w", k, parseErr)
			}
			obj[k] = n
		case k == "slots":
			slots = nil
			if e = json.Unmarshal([]byte(v), &slots); e != nil || slots == nil {
				return errors.New("invalid slots")
			}
			obj[k] = slots
		case strings.HasPrefix(k, "slots."):
			n, parseErr := strconv.Atoi(v)
			if parseErr != nil {
				return parseErr
			}
			slots[k[6:]] = n
			obj["slots"] = slots
		default:
			return fmt.Errorf("unknown setting %s", k)
		}
	}
	if e = validateHostSlots(root, slots); e != nil {
		return e
	}
	if e = writeJSON(dst, obj); e != nil {
		return e
	}
	if init {
		return nil
	}
	return json.NewEncoder(os.Stdout).Encode(obj)
}
func writeProbe(dir string) bool {
	f, e := createJobTemp(dir, ".cx-probe-")
	if e != nil {
		return false
	}
	return errors.Join(f.Close(), os.Remove(f.Name())) == nil
}
func doctorNode(ctx context.Context, root string) error {
	f := hostFacts(ctx, root)
	mux := "tmux"
	if runtime.GOOS == "windows" {
		var socket string
		mux, socket = psmuxSettings(root)
		f["psmux_session"] = probe(ctx, mux, "-L", socket, "list-sessions") != ""
		if cfg, err := readHostConfig(root); err == nil && cfg.MuxSession != "" {
			_, hasErr := exec.CommandContext(ctx, mux, "-L", socket, "has-session", "-t", cfg.MuxSession).Output()
			f["mux_session_present"] = hasErr == nil
		}
	}
	for _, tool := range []string{"git", "ssh", mux} {
		p, _ := exec.LookPath(tool)
		f[tool] = stringOrNil(p)
	}
	if exe, e := os.Executable(); e == nil {
		sum, e := fileHash(exe)
		if e != nil {
			return e
		}
		f["cx_version"] = sum
	}
	checkMaster()
	f["ssh_multiplexing"] = "ControlMaster=auto"
	if unsupportedMaster.Load() {
		f["ssh_multiplexing"] = "unsupported; using ordinary connections"
	}
	f["state_root"] = root
	f["state_writable"] = writeProbe(root)
	local, diskErr := stateDiskLocal(diskPath(root))
	f["state_local"] = local
	if diskErr != nil {
		f["state_disk_error"] = diskErr.Error()
	} else if !local {
		f["state_disk_error"] = "job state requires a local volume"
	}
	if runtime.GOOS == "windows" && local {
		indexErr := markStateNotIndexed(root)
		f["state_not_content_indexed"] = indexErr == nil
		if indexErr != nil {
			f["state_index_error"] = indexErr.Error()
		}
	}
	if runtime.GOOS == "linux" {
		f["state_owner_uid"] = stringOrNil(probe(ctx, "stat", "-c", "%u", root))
		f["effective_uid"] = os.Geteuid()
		f["state_owned_by_user"] = f["state_owner_uid"] == strconv.Itoa(os.Geteuid())
	}
	_, e := os.Stat(filepath.Join(root, hostConfigName))
	host, cfgErr := readHostConfig(root)
	f["host_json"] = e == nil && cfgErr == nil
	if cfgErr == nil {
		addDiskStatus(f, root, host)
		credentialDoctor(ctx, root, host, f)
	}
	cache := os.Getenv("GOCACHE")
	if cache == "" {
		cache = probe(ctx, "go", "env", "GOCACHE")
	}
	f["gocache"] = stringOrNil(cache)
	f["gocache_writable"] = cache != "" && writeProbe(cache)
	return json.NewEncoder(os.Stdout).Encode(f)
}
func jobDir(root, id string) (string, error) {
	p, j, ok := strings.Cut(id, "/")
	if !ok {
		return jobPath(root, id)
	}
	parent, e := jobPath(root, p)
	if e != nil || len(p) > 48 {
		return "", errors.New("invalid project")
	}
	return jobPath(parent, j)
}
func nodeControl(ctx context.Context, a []string) error {
	root, e := stateRoot()
	if e != nil {
		return e
	}
	if _, e := checkNodeStateFormat(root); e != nil {
		return e
	}
	switch a[0] {
	case "adopt-job":
		if len(a) == 3 {
			dir, err := jobDir(root, a[1])
			if err != nil {
				return err
			}
			return adoptJob(ctx, root, dir, a[2])
		}
	case "follow":
		if len(a) == 4 {
			dir, e := jobDir(root, a[1])
			if e != nil {
				return e
			}
			var pos tailPosition
			if e = json.Unmarshal([]byte(a[2]), &pos); e != nil {
				return e
			}
			limit, e := observation(a[3])
			if e != nil {
				return e
			}
			streamCtx, stop := context.WithTimeout(ctx, limit)
			defer stop()
			return streamLog(streamCtx, dir, pos)
		}
	case "observe-one":
		if len(a) == 3 {
			dir, e := jobDir(root, a[1])
			if e != nil {
				return e
			}
			limit, e := observation(a[2])
			if e != nil {
				return e
			}
			waitCtx, stop := context.WithTimeout(ctx, limit)
			defer stop()
			s, err := waitJob(waitCtx, dir, false)
			if err == nil || errors.As(err, new(stateFailure)) {
				if e = json.NewEncoder(os.Stdout).Encode(s.view(statusTime(ctx))); e != nil {
					return e
				}
			}
			return err
		}
	case "wait-ack":
		if len(a) == 5 {
			dir, err := jobDir(root, a[1])
			if err != nil {
				return err
			}
			return confirmWaitNotice(ctx, dir, jobIdentity{run: a[2], key: a[3]}, a[4])
		}
	case "submit-if-version":
		if len(a) > 4 {
			exe, e := os.Executable()
			if e != nil {
				return e
			}
			hash, e := fileHash(exe)
			if e != nil {
				return e
			}
			if hash != a[1] {
				return ctlExit(78)
			}
			return nodeControl(ctx, append([]string{"submit-command"}, a[2:]...))
		}
	case "install":
		if len(a) == 4 {
			return installNode(ctx, root, a[1], a[2], a[3])
		}
	case "host-config":
		return hostSettings(ctx, root, a[1:], false)
	case "check-profile":
		if len(a) == 2 {
			_, _, e := selectedProfile(root, a[1], true)
			return e
		}
	case "doctor":
		return doctorNode(ctx, root)
	case "overview":
		if len(a) == 4 {
			return nodeRows(ctx, root, a[1], a[2], a[3])
		}
	case "rows":
		if len(a) == 3 {
			return nodeRows(ctx, root, a[1], a[2], "")
		}
	case "show":
		if len(a) == 4 {
			dir, e := jobDir(root, a[1])
			if e != nil {
				return e
			}
			full, e := strconv.ParseBool(a[3])
			if e != nil {
				return e
			}
			return showNode(ctx, dir, a[2], full)
		}
	case "submit-command":
		if len(a) >= 4 {
			cwd, unlock, e := lockCommandWorkspace(ctx, root, a[1], a[2])
			if e != nil {
				return e
			}
			defer unlock()
			return submitNodeCommand(ctx, a, cwd)
		}
	case "wait-poll":
		if len(a) == 2 {
			var req waitPollRequest
			if err := json.Unmarshal([]byte(a[1]), &req); err != nil {
				return err
			}
			if req.Limit <= 0 {
				return errors.New("invalid poll limit")
			}
			if err := validateWaitProjects(req.Projects); err != nil {
				return err
			}
			for _, id := range req.Jobs {
				if err := validateWaitJob(id); err != nil {
					return err
				}
			}
			callCtx, cancel := timeshim.WithTimeout(ctx, timeshim.Or(contextClock(ctx)), req.Limit)
			defer cancel()
			var row *waitReport
			var err error
			cursor := req.Cursor
			if cursor == nil {
				cursor, err = initialWaitBaseline(callCtx, root, req.Session, req.FromStart)
			} else if err = validateWaitBaseline(cursor); err == nil {
				row, err = pollWaitAny(callCtx, root, req.Projects, req.Session, cursor, req.Jobs...)
			}
			if err != nil {
				if errors.Is(callCtx.Err(), context.DeadlineExceeded) {
					return errObserveTimeout
				}
				return err
			}
			return json.NewEncoder(os.Stdout).Encode(fields{"row": row, "cursor": cursor})
		}
	}
	return errors.New("invalid node arguments")
}

func checkSubmission(dir string, want config, profile string, args []string) error {
	var s status
	if err := loadJobStatusInto(dir, &s); err != nil {
		return err
	}
	var old config
	if e := readJobConfig(dir, &old); e != nil {
		return e
	}
	if s.Profile != profile || !maps.Equal(old.Env, want.Env) || !slices.Equal(old.Argv, args) || old.Dir != want.Dir || old.Session != want.Session || old.Timeout != want.Timeout || old.Quiet != want.Quiet || old.Shell {
		return errors.New("job already registered with different input")
	}
	return nil
}
func launchMuxCommand(ctx context.Context, window *muxWindow, root, p, j, runID, directory string) (func(), error) {
	exe, e := os.Executable()
	if e != nil {
		return nil, e
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	work := filepath.Join(root, ".work", p, j)
	argv := []string{"_command", "--state", root, "--project", p, "--job", j, "--expect-run", runID, "--expect-dir", directory}
	_, dispose, err := startMuxWorker(ctx, window, root, exe, work, argv, nil)
	return dispose, err
}

func lockCommandWorkspace(ctx context.Context, root, id, cwd string) (string, func(), error) {
	bound, e := bindWorkspace(root, cwd)
	if e != nil {
		return "", nil, e
	}
	unlock, e := bound.lock(ctx, root, id)
	if e != nil {
		return "", nil, e
	}
	if bound.path == "" {
		cwd, e = jobDir(filepath.Join(root, ".work"), id)
		if e != nil {
			unlock()
			return "", nil, e
		}
		if e = os.MkdirAll(cwd, 0700); e != nil {
			unlock()
			return "", nil, e
		}
		return cwd, unlock, nil
	}
	return bound.path, unlock, nil
}

func submitNodeCommand(ctx context.Context, a []string, cwd string) error {
	return runNode(ctx, append([]string{"start", "--cmd", "--job", a[1], "--dir", cwd}, a[3:]...), true)
}
