package main

import (
	"bytes"
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
	b, e := readStateFile(dst)
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
		case k == "load_cmd" || k == "psmux" || k == "mux_socket" || k == "env_file" || k == "default_profile":
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
	f, e := os.CreateTemp(dir, ".cx-probe-")
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
	}
	for _, tool := range []string{"git", "ssh", mux, "codex", "claude"} {
		p, _ := exec.LookPath(tool)
		f[tool] = stringOrNil(p)
	}
	var login struct {
		LoggedIn bool `json:"loggedIn"`
	}
	if json.Unmarshal([]byte(probe(ctx, "claude", "auth", "status", "--json")), &login) == nil {
		f["claude_logged_in"] = login.LoggedIn
	} else {
		f["claude_logged_in"] = nil
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
	if runtime.GOOS == "linux" {
		f["state_owner_uid"] = stringOrNil(probe(ctx, "stat", "-c", "%u", root))
		f["effective_uid"] = os.Geteuid()
		f["state_owned_by_user"] = f["state_owner_uid"] == strconv.Itoa(os.Geteuid())
	}
	_, e := os.Stat(filepath.Join(root, hostConfigName))
	host, cfgErr := readHostConfig(root)
	f["host_json"] = e == nil && cfgErr == nil
	if cfgErr == nil {
		addDiskStatus(f, diskPath(root), host)
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
	switch a[0] {
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
			s, err := waitJob(waitCtx, dir)
			if err == nil || errors.As(err, new(stateFailure)) || errors.Is(err, errStalled) {
				if e = json.NewEncoder(os.Stdout).Encode(s); e != nil {
					return e
				}
			}
			return err
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
			return showNode(dir, a[2], full)
		}
	case "submit-command":
		if len(a) >= 4 {
			lock, e := jobOperationLock(ctx, root, a[1])
			if e != nil {
				return e
			}
			defer closeBestEffort(lock)
			return submitNodeCommand(ctx, root, a)
		}
	case "wait-poll":
		if len(a) == 5 {
			limit, e := time.ParseDuration(a[4])
			if e != nil || limit <= 0 {
				return errors.New("invalid poll limit")
			}
			tmp, e := os.CreateTemp("", "cx-wait-*")
			if e != nil {
				return e
			}
			defer removeBestEffort(tmp.Name())
			_, e = tmp.WriteString(a[3])
			if e = errors.Join(e, tmp.Close()); e != nil {
				return e
			}
			callCtx, cancel := context.WithTimeout(ctx, limit)
			defer cancel()
			var b bytes.Buffer
			e = waitAny(callCtx, root, a[1], a[2], tmp.Name(), &b)
			if e != nil && !errors.Is(e, errQuestion) {
				return e
			}
			var cursor json.RawMessage
			if e = readJSON(tmp.Name(), &cursor); e != nil {
				return e
			}
			return json.NewEncoder(os.Stdout).Encode(fields{"row": json.RawMessage(b.Bytes()), "cursor": cursor})
		}
	}
	return errors.New("invalid node arguments")
}

func checkSubmission(dir string, want config, profile string, args []string) error {
	var s status
	if err := readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
		return err
	}
	var old config
	if e := readJSON(filepath.Join(dir, "config.json"), &old); e != nil {
		return e
	}
	if s.Profile != profile || !maps.Equal(old.Env, want.Env) || !slices.Equal(old.Argv, args) || old.Dir != want.Dir || old.Session != want.Session || old.Timeout != want.Timeout || old.Quiet != want.Quiet || old.Shell {
		return errors.New("job already registered with different input")
	}
	return nil
}
func launchMuxCommand(ctx context.Context, root, p, j, runID string) error {
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	program, socket := psmuxSettings(root)
	cmd := exec.CommandContext(ctx, program, "-L", socket, "new-session", "-d", "-s", "cx-"+runID, "--", exe, "_command", "--state", root, "--project", p, "--job", j)
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	return errors.Join(err, ctx.Err())
}

func submitNodeCommand(ctx context.Context, root string, a []string) error {
	dir, e := jobDir(root, a[1])
	if e != nil {
		return e
	}
	cwd := a[2]
	if cwd == "" {
		cwd = filepath.Join(root, ".work", filepath.Base(filepath.Dir(dir)), filepath.Base(dir))
		if e = os.MkdirAll(cwd, 0700); e != nil {
			return e
		}
	}
	abs, e := filepath.Abs(cwd)
	if e != nil {
		return e
	}
	return runNode(ctx, append([]string{"start", "--cmd", "--job", a[1], "--dir", abs}, a[3:]...), true)
}
