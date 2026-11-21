package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func usage(f fields, raw json.RawMessage) {
	var u struct {
		ModelContextWindow float64
		Last, Total        struct{ TotalTokens *int64 }
	}
	_ = json.Unmarshal(raw, &u)
	f["tokens"] = u.Total.TotalTokens
	f["context_percent"] = nil
	if u.ModelContextWindow > 0 && u.Last.TotalTokens != nil {
		f["context_percent"] = math.RoundToEven(float64(*u.Last.TotalTokens)*1000/u.ModelContextWindow) / 10
	}
}
func workDir(dir string, s status) (string, error) {
	var work string
	e := readJSON(filepath.Join(dir, "cx-work.json"), &work)
	if os.IsNotExist(e) {
		root := filepath.Dir(dir)
		if s.Project != "" {
			root = filepath.Dir(root)
		}
		private := filepath.Join(root, ".work", s.Project, s.Job)
		if _, err := os.Stat(filepath.Join(private, "request.json")); err == nil {
			return private, nil
		} else if !os.IsNotExist(err) {
			return "", err
		}
		for _, base := range []string{os.Getenv("CX_HOME"), filepath.Dir(filepath.Dir(filepath.Dir(dir)))} {
			if base == "" {
				continue
			}
			for _, prefix := range []string{filepath.Join(".locks", "cx-executor", "cx-v2", "jobs"), filepath.Join("cx-v2", "jobs")} {
				work := filepath.Join(base, prefix, s.Project, s.Job)
				if fi, e := os.Stat(work); e == nil && fi.IsDir() {
					return work, nil
				}
			}
		}
		return dir, nil
	}
	return work, e
}
func nodeRows(ctx context.Context, root, p, session string, chatProject string) error {
	overview := chatProject != ""
	rows := []fields{}
	var rate any
	var latest string
	e := scanJobs(ctx, root, p, session, func(s status) (bool, error) {
		b, e := json.Marshal(briefStatus(s))
		if e != nil {
			return false, e
		}
		var r fields
		if e = decodeRows(b, &r); e != nil {
			return false, e
		}
		usage(r, s.Usage)
		if len(s.Usage) > 0 && string(s.Usage) != "null" {
			r["token_usage"] = s.Usage
		}
		work, e := workDir(filepath.Join(root, s.Project, s.Job), s)
		if e != nil {
			return false, e
		}
		var meta struct {
			Role, Parent string
			Privileged   bool
		}
		if e = readJSON(filepath.Join(work, "request.json"), &meta); e != nil && !os.IsNotExist(e) {
			return false, e
		}
		if meta.Role == "" {
			meta.Role = "worker"
		}
		r["role"] = meta.Role
		if overview {
			r["token_usage"] = s.Usage
			if meta.Parent == "" {
				meta.Parent = s.Session
			}
			r["parent"] = meta.Parent
			r["waiting_input"] = s.State == "waiting_input"
			r["error"] = s.FailedCommand != nil || isTerminal(s.State) && s.State != "done"
			stamp := s.LastEvent.UTC().Format("20060102150405.000000000")
			if stamp >= latest {
				latest = stamp
				rate = s.rateLimitStatus
			}
		} else {
			r["privileged"] = s.Privileged || meta.Privileged
		}
		rows = append(rows, r)
		return false, nil
	})
	if e != nil {
		return e
	}
	if !overview {
		return json.NewEncoder(os.Stdout).Encode(rows)
	}
	f := hostFacts(ctx, root)
	h, e := readHostConfig(root)
	if e != nil {
		return e
	}
	addDiskStatus(f, diskPath(root), h)
	slots := fields{}
	for k, cap := range h.Slots {
		r := runner{root: root}
		r.c.SlotClass = k
		matches, e := filepath.Glob(filepath.Join(r.pool(), "slot-*.lock"))
		if e != nil {
			return e
		}
		used := 0
		for _, path := range matches {
			lock, e := lockPath(path, 0)
			if e != nil {
				return e
			}
			if lock == nil {
				used++
			} else {
				lock.Close()
			}
		}
		slots[k] = fields{"used": used, "capacity": cap}
	}
	f["slots"] = slots
	f["rate_limit"] = rate
	f["unread"] = nil
	if base := os.Getenv("CX_CHAT_LOCAL_ROOT"); base != "" {
		tips, e := chatTips(ctx, base, chatProject)
		if e != nil {
			return e
		}
		f["channel_tips"] = tips
	}
	f["jobs"] = rows
	return json.NewEncoder(os.Stdout).Encode(f)
}

var decisionHeading = regexp.MustCompile(`(?m)^(?:#{1,6}[ \t]+)?(?:\*\*)?(?:Pending decisions|Decisions to confirm|待确认决策)(?:\*\*)?[：:]?[ \t]*$`)
var nextHeading = regexp.MustCompile(`(?m)^#{1,2}[ \t]+`)

func showNode(dir, raw string, full bool) error {
	clip := func(s string) string {
		r := []rune(s)
		if full || len(r) <= 240 {
			return s
		}
		return string(r[:119]) + " … " + string(r[len(r)-118:])
	}

	var s status
	if e := readJSON(filepath.Join(dir, "status.json"), &s); e != nil {
		return e
	}
	current := s.Finished
	if s.TurnStatus == "inProgress" {
		current++
	}
	n := current
	var e error
	if raw != "" {
		n, e = strconv.ParseInt(raw, 10, 64)
		if e != nil || n < 0 {
			return errors.New("invalid turn")
		}
	}
	work, e := workDir(dir, s)
	if e != nil {
		return e
	}
	archive := filepath.Join(work, "result", fmt.Sprintf("turn-%d", n))
	var manifest struct {
		TurnID       string `json:"turn_id"`
		Repositories []struct{ Files int }
	}
	e = readJSON(filepath.Join(archive, "result.json"), &manifest)
	sealed := e == nil
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	if !sealed && n != current {
		return fmt.Errorf("turn %d unavailable", n)
	}
	id := manifest.TurnID
	if id == "" && n == current {
		id = s.Turn
	}
	plan := []fields{}
	commands := []fields{}
	index := map[string]int{}
	paths := map[string]bool{}
	changed := 0
	f, e := os.Open(filepath.Join(dir, "events.jsonl"))
	if e == nil {
		defer f.Close()
		rd := bufio.NewReader(f)
		for {
			line, e := rd.ReadBytes('\n')
			if e == io.EOF {
				break
			}
			if e != nil {
				return e
			}
			var ev struct {
				Method string
				Params struct {
					TurnID, Diff string
					Plan         []fields
					Item         struct {
						ID, Type, Command, Text, AggregatedOutput string
						ExitCode                                  *int
						Changes                                   []struct{ Path string }
					}
				}
			}
			if e = json.Unmarshal(line, &ev); e != nil {
				return e
			}
			v := ev.Params
			if id == "" || v.TurnID != id {
				continue
			}
			switch ev.Method {
			case "turn/plan/updated":
				plan = v.Plan
			case "turn/diff/updated":
				changed = strings.Count("\n"+v.Diff, "\ndiff --git ")
				if changed == 0 {
					changed = strings.Count("\n"+v.Diff, "\n+++ ")
				}
			case "item/started", "item/completed":
				it := v.Item
				switch it.Type {
				case "commandExecution":
					j, ok := index[it.ID]
					if !ok {
						j = len(commands)
						index[it.ID] = j
						commands = append(commands, nil)
					}
					commands[j] = fields{"command": clip(it.Command), "exit_code": it.ExitCode}
					if it.AggregatedOutput != "" {
						commands[j]["output"] = clip(it.AggregatedOutput)
					}
				case "fileChange":
					for _, c := range it.Changes {
						paths[c.Path] = true
					}
				case "plan":
					if len(plan) == 0 && it.Text != "" {
						plan = append(plan, fields{"step": it.Text})
					}
				}
			}
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	report := ""
	if sealed || n == s.Finished {
		folder := dir
		if sealed {
			folder = archive
		}
		b, e := os.ReadFile(filepath.Join(folder, "last.md"))
		if e != nil && !os.IsNotExist(e) {
			return e
		}
		report = string(b)
		if !sealed {
			var after status
			if e = readJSON(filepath.Join(dir, "status.json"), &after); e != nil {
				return e
			}
			if s.Turn != after.Turn || s.Finished != after.Finished || s.TurnStatus != after.TurnStatus {
				return errors.New("turn advanced during show")
			}
		}
	}
	if s.Engine == "cmd" {
		var conf config
		if e = readJSON(filepath.Join(dir, "config.json"), &conf); e != nil {
			return e
		}
		var spec struct {
			Direct bool     `json:"direct_exec"`
			Argv   []string `json:"command_argv"`
		}
		if e = readJSON(filepath.Join(work, "request.json"), &spec); e != nil && !os.IsNotExist(e) {
			return e
		}
		if spec.Direct {
			conf.Argv = spec.Argv
		}
		parts := make([]string, len(conf.Argv))
		for i, arg := range conf.Argv {
			var b strings.Builder
			enc := json.NewEncoder(&b)
			enc.SetEscapeHTML(false)
			if e = enc.Encode(arg); e != nil {
				return e
			}
			parts[i] = strings.TrimSuffix(b.String(), "\n")
		}
		argv := "[" + strings.Join(parts, ", ") + "]"
		cmd := fields{"command": clip(argv), "exit_code": s.ExitCode}
		if b, e := os.ReadFile(filepath.Join(dir, "output.log")); e == nil {
			cmd["output"] = clip(string(b))
		} else if !os.IsNotExist(e) {
			return e
		}
		commands = append(commands, cmd)
	}
	changed = max(changed, len(paths))
	if len(manifest.Repositories) > 0 {
		changed = 0
		for _, r := range manifest.Repositories {
			changed += r.Files
		}
	}
	decisions := ""
	if span := decisionHeading.FindStringIndex(report); span != nil {
		end := len(report)
		if next := nextHeading.FindStringIndex(report[span[1]:]); next != nil {
			end = span[1] + next[0]
		}
		decisions = report[span[0]:end]
	}
	if s.Engine == "cmd" {
		report = clip(report)
	}
	out := fields{"turn": n, "report": report, "decisions": decisions, "plan": plan, "commands": commands, "changed_files": changed, "token_usage": s.Usage}
	usage(out, s.Usage)
	return json.NewEncoder(os.Stdout).Encode(out)
}

func chatTips(ctx context.Context, base, p string) (map[string]int64, error) {
	if _, e := jobPath("", p); e != nil {
		return nil, e
	}
	repo := filepath.Join(base, p+"-meta.git")
	tips := map[string]int64{}
	if _, e := os.Stat(repo); os.IsNotExist(e) {
		return tips, nil
	} else if e != nil {
		return nil, e
	}
	ctx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	b, e := exec.CommandContext(ctx, "git", "--git-dir", repo, "for-each-ref", "--format=%(refname:strip=3)%09%(subject)", "refs/cx/chat/").Output()
	if e != nil {
		return nil, e
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		ch, raw, ok := strings.Cut(line, "\t")
		if !ok {
			return nil, errors.New("invalid channel ref")
		}
		var msg struct{ Seq int64 }
		if e = json.Unmarshal([]byte(raw), &msg); e != nil {
			return nil, e
		}
		tips[ch] = msg.Seq
	}
	return tips, nil
}
func chatCursor(p, ch string) (int64, error) {
	path, e := channelCursorPath(p, ch, "default")
	if e != nil {
		return 0, e
	}
	var n int64
	e = readJSON(path, &n)
	if os.IsNotExist(e) {
		return 0, nil
	}
	return n, e
}

func channelCursorPath(p, ch, name string) (string, error) {
	for _, v := range []string{p, ch, name} {
		if _, e := jobPath("", v); e != nil {
			return "", e
		}
	}
	base := os.Getenv("CX_HOME")
	if base == "" {
		base, _ = os.Getwd()
	}
	legacy := filepath.Join(base, ".locks", "cx-jobs", ".chat", p, ch, name+".json")
	if os.Getenv("CX_HOME") != "" {
		return legacy, nil
	}
	if _, e := os.Stat(legacy); e == nil {
		return legacy, nil
	} else if !os.IsNotExist(e) {
		return "", e
	}
	return cursorFile("chat", p, ch, name)
}
