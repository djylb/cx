package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

type appliedJob struct {
	Host, Job string
	RunID     string `json:"run_id"`
}

type appliedRepo struct {
	Source              string `json:"source"`
	Operation           string `json:"operation"`
	TargetRepository    string `json:"target_repository"`
	TargetRepositoryKey string `json:"target_repository_key"`
	Repo                string
	TargetHead          *string `json:"target_head_at_apply"`
	ResultTree          string  `json:"result_tree"`
	Turn                int64
}

type applyRecord struct {
	appliedJob
	appliedRepo
	Changeset string
	Role      string
	Baseline  SnapshotID
	Session   string
	Modules   []string
}

func readApplyLog(project string, visit func(applyRecord)) error {
	dir, err := projectOutputPath(project)
	if err != nil {
		return err
	}
	log, err := openJobFile(filepath.Join(dir, "apply-log.jsonl"), jobFileRead)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer closeBestEffort(log)
	dec := json.NewDecoder(bufio.NewReader(log))
	for {
		var entry applyRecord
		if err = dec.Decode(&entry); errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		visit(entry)
	}
}

func readAppliedResults(project string) (map[appliedJob]map[string]appliedRepo, error) {
	turns := map[appliedJob]map[string]appliedRepo{}
	err := readApplyLog(project, func(entry applyRecord) {
		key := entry.appliedJob
		if turns[key] == nil {
			turns[key] = map[string]appliedRepo{}
		}
		noteAppliedRepository(turns[key], entry.appliedRepo)
	})
	return turns, err
}

func noteAppliedRepository(byRepo map[string]appliedRepo, entry appliedRepo) {
	if previous, ok := byRepo[entry.Repo]; !ok || entry.Turn >= previous.Turn {
		byRepo[entry.Repo] = entry
	}
}

const appliedTurnHeading = "APPLIED TURN"

func appliedTurnColumn(row fields) any {
	turns, _ := row["applied_turn"].(map[string]int64)
	if len(turns) == 0 {
		return nil
	}
	values := make([]string, 0, len(turns))
	for _, repo := range slices.Sorted(maps.Keys(turns)) {
		values = append(values, repo+"="+strconv.FormatInt(turns[repo], 10))
	}
	return strings.Join(values, ",")
}

func workDir(dir string, s status) (string, error) {
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
	return dir, nil
}
func nodeRows(ctx context.Context, root, p, session, chatProject string) error {
	overview := chatProject != ""
	now := statusTime(ctx)
	rows := make([]fields, 0) // Encode an empty list as [].
	var rate any
	var latest string
	e := scanJobs(ctx, root, p, session, func(s status) (bool, error) {
		dir := filepath.Join(root, s.Project, s.Job)
		b, e := json.Marshal(briefStatus(ctx, dir, s, now))
		if e != nil {
			return false, e
		}
		var r fields
		if e = decodeRows(b, &r); e != nil {
			return false, e
		}
		usage(r, s.Usage)
		r["turn_context_percent"] = s.TurnContext
		if len(s.Usage) > 0 && string(s.Usage) != "null" {
			r["token_usage"] = s.Usage
		}
		work, e := workDir(dir, s)
		if e != nil {
			return false, e
		}
		var meta struct {
			moduleWork
			Snapshot     SnapshotID
			Role, Parent string
			Privileged   bool
		}
		request, e := readSharedFile(filepath.Join(work, "request.json"))
		if e == nil {
			if e = json.Unmarshal(request, &meta); e != nil {
				return false, e
			}
		} else {
			if !os.IsNotExist(e) {
				return false, e
			}
			// Forks inherit their parent's runner configuration.
			if s.Engine != "" && s.Engine != "cmd" {
				var c config
				if err := readJobConfig(dir, &c); err != nil {
					return false, err
				}
				if c.Work != nil {
					meta.moduleWork = *c.Work
				}
			}
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
				rate = s.rateLimitView(now)
			}
		} else {
			r["privileged"] = s.Privileged || meta.Privileged
			r["run_id"] = s.RunID
			if meta.Snapshot != "" {
				base, baseErr := readJobBaseline(work)
				if baseErr != nil {
					return false, baseErr
				}
				if base.Snapshot == "" {
					base.Snapshot = meta.Snapshot
					if info, statErr := os.Stat(filepath.Join(work, "input", "manifest.json")); statErr == nil {
						base.Time = info.ModTime().UTC()
					}
				}
				r["baseline"], r["baseline_at"], r["baseline_from_turn"] = base.Snapshot, base.Time, base.FromTurn
				if base.Work != nil {
					meta.moduleWork = *base.Work
				}
			}
		}
		meta.listing().addModuleListing(r)
		rows = append(rows, r)
		return false, nil
	})
	if e != nil {
		return e
	}
	h, e := readHostConfig(root)
	if !overview {
		if e == nil && len(rows) > 0 {
			f := fields{}
			addCacheWarning(f, root, h.cacheMaxBytes(h.diskStatus(diskPath(root)).total))
			if warning := f["cache_warning"]; warning != nil {
				for _, row := range rows {
					row["cache_warning"] = warning
				}
			}
		}
		return json.NewEncoder(os.Stdout).Encode(rows)
	}
	if e != nil {
		return e
	}
	f := hostFacts(ctx, root)
	addDiskStatus(f, root, h)
	slots := fields{}
	for k, capacity := range h.Slots {
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
				_ = lock.Close()
			}
		}
		slots[k] = fields{"used": used, "capacity": capacity}
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

func showNode(ctx context.Context, dir, raw string, full bool) error {
	clip := func(s string) string {
		r := []rune(s)
		if full || len(r) <= 240 {
			return s
		}
		return string(r[:119]) + " … " + string(r[len(r)-118:])
	}

	var s status
	if e := loadJobStatusInto(dir, &s); e != nil {
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
	var sealed bool
	currentID := currentTurnID(s)
	if raw != "" || currentID != "" {
		e = readJSON(filepath.Join(archive, "result.json"), &manifest)
		sealed = e == nil && (raw != "" || manifest.TurnID == currentID)
		if e != nil && !os.IsNotExist(e) {
			return e
		}
	}
	if !sealed {
		manifest.TurnID, manifest.Repositories = "", nil
	}
	if !sealed && n != current {
		return fmt.Errorf("turn %d unavailable", n)
	}
	id := manifest.TurnID
	if id == "" && n == current {
		id = currentID
	}
	// Preserve empty JSON arrays when the turn has no recorded plan or commands.
	plan := make([]fields, 0)
	commands := make([]fields, 0)
	index := map[string]int{}
	paths := map[string]bool{}
	changed := 0
	f, e := openJobFile(filepath.Join(dir, "events.jsonl"), jobFileRead)
	if e == nil {
		defer closeBestEffort(f)
		rd := bufio.NewReader(f)
		for {
			line, eventErr := rd.ReadBytes('\n')
			if eventErr == io.EOF {
				break
			}
			if eventErr != nil {
				return eventErr
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
			if eventErr = json.Unmarshal(line, &ev); eventErr != nil {
				return eventErr
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
	if sealed {
		b, err := readSharedFile(filepath.Join(archive, "last.md"))
		if err != nil {
			return err
		}
		report = string(b)
	} else if n == s.Finished && id == s.LastCompleted {
		b, err := readTurnReport(ctx, dir, s.LastCompleted)
		if err != nil {
			return err
		}
		report = string(b)
	}
	if s.Engine == "cmd" {
		var conf config
		if e = readJobConfig(dir, &conf); e != nil {
			return e
		}
		var spec struct {
			Argv   []string `json:"command_argv"`
			Direct bool     `json:"direct_exec"`
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
		if b, e := readSharedFile(filepath.Join(dir, "output.log")); e == nil {
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
	if s.Profile != "" {
		out["profile"] = s.Profile
	}
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
	for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
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
	return cursorFile("chat", p, ch, name)
}

func addAppliedListing(row fields, repos map[string]appliedRepo, floor int64) {
	turns := make(map[string]int64)
	heads := make(map[string]*string)
	trees := make(map[string]string)
	for repo, result := range repos {
		if result.Turn < floor {
			continue
		}
		turns[repo] = result.Turn
		if result.ResultTree != "" || result.TargetHead != nil {
			heads[repo] = result.TargetHead
			trees[repo] = result.ResultTree
		}
	}
	if len(turns) > 0 {
		row["applied_turn"] = turns
	}
	if len(heads) > 0 {
		row["target_head_at_apply"] = heads
		row["result_tree"] = trees
	}
}
