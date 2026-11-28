package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/djylb/timeshim"
)

type hostReply struct {
	e         error
	name      string
	sampledAt time.Time
	r         reply
}

func listingJobID(row fields) string {
	return strings.TrimLeft(fmt.Sprint(row["project"], "/", row["job"]), "/")
}

func mergeSubmissionRows(ctx context.Context, host, filter string, o ctlOptions, rows []fields) ([]fields, error) {
	root, err := submissionRoot(host)
	if err != nil {
		return nil, err
	}
	projects, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return rows, nil
	}
	if err != nil {
		return nil, err
	}
	registered := make(map[string]bool, len(rows))
	for _, row := range rows {
		registered[listingJobID(row)] = true
	}
	for _, project := range projects {
		p := project.Name()
		if !project.IsDir() || o.project != "" && p != o.project {
			continue
		}
		jobs, readErr := os.ReadDir(filepath.Join(root, p))
		if os.IsNotExist(readErr) {
			continue
		}
		if readErr != nil {
			return nil, readErr
		}
		for _, job := range jobs {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			id := p + "/" + job.Name()
			if !job.IsDir() || registered[id] || filter != "" && id != filter {
				continue
			}
			dir, pathErr := jobDir(root, id)
			if pathErr != nil {
				continue
			}
			s, progressErr := readSubmissionProgress(dir)
			if os.IsNotExist(progressErr) {
				continue
			}
			if progressErr != nil {
				return nil, progressErr
			}
			if s.Host != host || s.Project != p || s.Job != job.Name() ||
				o.session != "" && s.Session != o.session ||
				s.State != "submitting" && s.State != "submit_pending" && s.State != "submit_unverified" && s.State != "submit_failed" {
				continue
			}
			data, marshalErr := json.Marshal(s)
			if marshalErr != nil {
				return nil, marshalErr
			}
			var row fields
			if decodeErr := decodeRows(data, &row); decodeErr != nil {
				return nil, decodeErr
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

type lsSnapshot struct {
	AsOf time.Time `json:"as_of"`
	Jobs []fields  `json:"jobs"`
}

func lsCachePath(host string) (string, error) {
	home, err := controllerCacheRoot()
	if err != nil {
		return "", err
	}
	path, err := jobPath(filepath.Join(home, "ls"), host)
	if err != nil {
		return "", err
	}
	return path + ".json", nil
}

var errInvalidLSSnapshot = errors.New("invalid listing snapshot")

func loadLSSnapshot(path string) (lsSnapshot, error) {
	var snapshot lsSnapshot
	data, err := readSharedFile(path)
	if err != nil {
		return snapshot, err
	}
	if err := decodeRows(data, &snapshot); err != nil {
		return snapshot, fmt.Errorf("%w: %w", errInvalidLSSnapshot, err)
	}
	if snapshot.AsOf.IsZero() || snapshot.Jobs == nil || slices.ContainsFunc(snapshot.Jobs, func(row fields) bool { return row == nil }) {
		return snapshot, errInvalidLSSnapshot
	}
	return snapshot, nil
}

func saveLSSnapshot(ctx context.Context, path string, snapshot lsSnapshot) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	lock, err := lockUntil(ctx, path+".lock")
	if err != nil {
		return err
	}
	defer closeBestEffort(lock)
	current, err := loadLSSnapshot(path)
	switch {
	case err == nil:
		if !snapshot.AsOf.After(current.AsOf) {
			return nil
		}
	case !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errInvalidLSSnapshot):
		return err
	}
	return writeJSON(path, snapshot)
}

func activeListState(state any) bool {
	switch state {
	case "running", "queued", "submitting", "waiting":
		return true
	default:
		return false
	}
}

func displayHosts(ctx context.Context, hs endpoints, op string, o ctlOptions) error {
	if err := validateUsageOptions(o); err != nil {
		return err
	}
	var listingSettings usageSettings
	if o.usage {
		var err error
		listingSettings, err = usageListSettings(o)
		if err != nil {
			return err
		}
	}
	if op == "ls" && o.changeset != "" {
		return displayChangeset(ctx, hs, o)
	}
	names := hs.names()
	if o.active {
		names = slices.Sorted(maps.Keys(hs))
	}
	filter := ""
	if op == "doctor" && len(o.pos) > 0 {
		names = o.pos
	} else if op == "ls" && len(o.pos) == 1 {
		host, id, err := resolveJob(ctx, o.pos[0])
		if err != nil {
			return err
		}
		names = []string{host}
		filter = id
		o.project, _, _ = strings.Cut(id, "/")
	} else if len(o.pos) > 0 {
		return errors.New("unexpected arguments")
	}
	chatProject := o.project
	if op == "overview" && chatProject == "" {
		var e error
		chatProject, e = defaultProject(ctx)
		if e != nil {
			return e
		}
	}
	prev := map[string]string{}
	header := true
	for ctx.Err() == nil {
		ch := make(chan hostReply, len(names))
		for _, name := range names {
			h, e := hs.get(name)
			if e != nil {
				return e
			}
			go func() {
				if op == "doctor" && h.SSH != "" && h.Platform == "" {
					plat, err := detectPlatform(ctx, h)
					if err != nil {
						ch <- hostReply{name: name, e: err}
						return
					}
					h = h.withPlatform(plat)
				}
				args := []string{"doctor"}
				if op != "doctor" {
					args = []string{op, o.project, o.session}
					if op == "ls" {
						// Fetch a complete snapshot so it can serve any offline filter.
						args = []string{"rows", "", ""}
					}
					if op == "overview" {
						args = append(args, chatProject)
					}
				}
				r, e := h.call(ctx, 30*time.Second, args...)
				if op == "doctor" && e == nil && r.code == missingNodeBinary && h.SSH != "" {
					var f fields
					f, e = preinstallDoctor(ctx, h)
					if e == nil {
						r.data, e = json.Marshal(f)
						r.code = 0
					}
				}
				v := hostReply{name: name, r: r, e: e}
				if op == "ls" && e == nil && r.code == 0 {
					v.sampledAt = timeshim.Or(contextClock(ctx)).Now().UTC()
				}
				ch <- v
			}()
		}
		results := map[string]hostReply{}
		for range names {
			v := <-ch
			results[v.name] = v
		}
		reports := map[string]fields{}
		var output bytes.Buffer // In-memory formatting cannot return a write error.
		enc := json.NewEncoder(&output)
		failed := false
		var board *usageBoard
		if o.usage {
			board = newUsageBoard(listingSettings)
		}
		current := map[string]string{}
		appliedByProject := map[string]map[appliedJob]map[string]appliedRepo{}
		for _, name := range names {
			v := results[name]
			nodeFailed := v.e != nil || v.r.code != 0
			if nodeFailed {
				failed = true
				reports[name] = fields{"error": fmt.Sprintf("node error %v (code %d)", v.e, v.r.code), "jobs": []fields{}}
				if op != "ls" {
					if op == "doctor" {
						addRecentTransport(name, reports[name])
					}
					continue
				}
			}
			if op != "ls" {
				var obj fields
				if e := decodeRows(v.r.data, &obj); e != nil {
					return e
				}
				if tips, ok := obj["channel_tips"].(map[string]any); ok {
					unread := map[string]int64{}
					for channel, tip := range tips {
						n, e := tip.(json.Number).Int64()
						if e != nil {
							return e
						}
						last, e := chatCursor(chatProject, channel)
						if e != nil {
							return e
						}
						unread[channel] = max(0, n-last)
					}
					obj["unread"] = unread
					delete(obj, "channel_tips")
				}
				if op == "doctor" {
					addRecentTransport(name, obj)
				}
				if op == "doctor" && unsupportedMaster.Load() {
					obj["controller_ssh_multiplexing"] = "unsupported; using ordinary connections"
				}
				reports[name] = obj
				continue
			}
			var rows []fields
			var asOf string
			path, cacheErr := lsCachePath(name)
			if nodeFailed {
				if cacheErr == nil {
					var snapshot lsSnapshot
					snapshot, cacheErr = loadLSSnapshot(path)
					if cacheErr == nil {
						rows = snapshot.Jobs
						asOf = snapshot.AsOf.Format(time.RFC3339Nano)
					}
				}
				if asOf != "" {
					diagnosticf("%s: unreachable (last successful observation: %s; %v)\n", name, asOf, reports[name]["error"])
				} else {
					diagnosticf("%s: unreachable (%v)\n", name, reports[name]["error"])
				}
			} else {
				if e := decodeRows(v.r.data, &rows); e != nil {
					return e
				}
				if cacheErr == nil {
					cacheErr = saveLSSnapshot(ctx, path, lsSnapshot{
						AsOf: v.sampledAt,
						Jobs: rows,
					})
				}
			}
			if cacheErr != nil && (!nodeFailed || !errors.Is(cacheErr, os.ErrNotExist)) {
				diagnosticf("%s: ls cache: %v\n", name, cacheErr)
			}
			rows = slices.DeleteFunc(rows, func(row fields) bool {
				return (o.project != "" && row["project"] != o.project) ||
					(o.session != "" && row["session"] != o.session)
			})
			if asOf != "" {
				for _, row := range rows {
					row["stale"] = true
					row["as_of"] = asOf
					row["host"] = name
				}
			}
			rows, e := mergeSubmissionRows(ctx, name, filter, o, rows)
			if e != nil {
				return e
			}
			for _, row := range rows {
				id := listingJobID(row)
				if (o.active && !activeListState(row["status"])) || filter != "" && id != filter || !matchesModuleListing(row, o) {
					continue
				}
				key := name + ":" + id
				role := row["role"]
				if o.role != "" && role != o.role {
					continue
				}
				project, _ := row["project"].(string)
				if board != nil {
					nameRole, _ := role.(string)
					board.add(name, project, id, nameRole, row)
					continue
				}
				runID, _ := row["run_id"].(string)
				delete(row, "run_id")
				delete(row, "module_claims")
				delete(row, "module_repo")
				if project != "" {
					applied, loaded := appliedByProject[project]
					if !loaded {
						applied, e = readAppliedResults(project)
						if e != nil {
							diagnosticf("%s: apply log: %v\n", project, e)
						}
						appliedByProject[project] = applied
					}
					if turns := applied[appliedJob{Host: name, Job: id, RunID: runID}]; runID != "" && len(turns) != 0 {
						number, _ := row["baseline_from_turn"].(json.Number)
						floor, _ := number.Int64()
						addAppliedListing(row, turns, floor)
					}
				}
				delete(row, "role")
				priv := row["privileged"]
				delete(row, "privileged")
				if o.watch {
					fp, _ := json.Marshal([]any{row, role, priv, runID})
					current[key] = string(fp)
					if prev[key] == current[key] {
						continue
					}
					row["host"] = name
				}
				if o.json {
					if e := enc.Encode(row); e != nil {
						return e
					}
				} else {
					state := fmt.Sprint(row["status"])
					if row["cache_warning"] != nil {
						state += " (pinned cache over budget)"
					}
					if priv == true {
						state += " privileged"
					}
					if row["runner_outdated"] == true {
						state += " (runner predates installed cx: close and resume to update)"
					}
					if row["stale"] == true {
						state += " (unreachable, last successful observation: " + asOf + ")"
					}
					renderLSRow(&output, &header, o.brief, id, name, role, state, row)
				}
			}
		}
		if board != nil {
			if err := board.write(&output, o.json); err != nil {
				return err
			}
		}
		if o.watch && board == nil {
			for k, old := range prev {
				if _, ok := current[k]; ok {
					continue
				}
				name, id, _ := strings.Cut(k, ":")
				if v := results[name]; v.e != nil || v.r.code != 0 {
					current[k] = old
					continue
				}
				p, j, ok := strings.Cut(id, "/")
				if !ok {
					j = p
					p = ""
				}
				if o.json {
					if e := enc.Encode(fields{"host": name, "project": p, "job": j, "status": "removed"}); e != nil {
						return e
					}
				} else {
					renderLSRow(&output, &header, o.brief, id, name, nil, "removed", nil)
				}
			}
		}
		if op == "overview" {
			nest(reports, names)
		}
		if op != "ls" {
			if o.json {
				if e := enc.Encode(reports); e != nil {
					return e
				}
			} else {
				for _, name := range names {
					obj := reports[name]
					jobs := obj["jobs"]
					delete(obj, "jobs")
					b, _ := json.MarshalIndent(obj, "", "  ")
					_, _ = fmt.Fprintf(&output, "%s %s\n", name, b)
					if rows, ok := jobs.([]fields); ok {
						var previousSession string
						for i, r := range rows {
							session, _ := r["session"].(string)
							if i == 0 || session != previousSession {
								if session == "" {
									output.WriteString("  No session\n")
								} else {
									_, _ = fmt.Fprintf(&output, "  %q\n", session)
								}
								previousSession = session
							}
							_, _ = fmt.Fprintf(&output, "%s%s/%s %s %s %v", strings.Repeat("  ", 2+r["depth"].(int)), r["project"], r["job"], r["role"], r["status"], r["plan_step"])
							if profile, ok := r["profile"].(string); ok {
								_, _ = fmt.Fprintf(&output, " profile=%s", profile)
							}
							output.WriteByte('\n')
						}
					}
				}
			}
		}
		if e := preview(output.Bytes(), o); e != nil {
			return e
		}
		if !o.watch {
			if failed {
				return ctlExit(1)
			}
			return nil
		}
		prev = current
		if e := pause(ctx, o.interval); e != nil {
			return e
		}
	}
	return ctx.Err()
}
func renderLSRow(output *bytes.Buffer, header *bool, brief bool, id, name string, role any, state string, row fields) {
	var failure any
	if v, ok := row["failed_command"].(map[string]any); ok {
		failure = fmt.Sprintf("%v (%v)", v["command"], v["exit_code"])
	}
	var rate any
	if row["rate_limit"] != nil {
		b, _ := json.Marshal(row["rate_limit"])
		rate = string(b)
	}
	columns := []struct {
		value any
		name  string
		brief bool
	}{
		{id, "JOB", true},
		{name, "HOST", true},
		{role, "ROLE", false},
		{state, "STATUS", true},
		{row["phase"], "PHASE", true},
		{strings.Join(listingModules(row), ","), "MODULE", true},
		{row["completed_turns"], "TURNS", true},
		{appliedTurnColumn(row), appliedTurnHeading, true},
		{baselineColumn(row), "BASELINE", true},
		{row["plan_step"], "PLAN", true},
		{row["changed_files"], "FILES", false},
		{failure, "FAILED COMMAND", false},
		{rate, "RATE LIMIT", true},
		{row["cached_tokens"], "CACHED", false},
		{row["fresh_input_tokens"], "FRESH INPUT", false},
		{row["output_tokens"], "OUTPUT", false},
		{row["tokens"], "TOKENS", false},
		{row["context_percent"], "CONTEXT %", false},
		{row["profile"], "PROFILE", false},
	}
	var headings, values []string
	for _, column := range columns {
		if brief && !column.brief {
			continue
		}
		headings = append(headings, column.name)
		value := "-"
		if column.value != nil {
			value = strings.Join(strings.Fields(fmt.Sprint(column.value)), " ")
		}
		values = append(values, value)
	}
	if *header {
		output.WriteString(strings.Join(headings, "\t") + "\n")
		*header = false
	}
	output.WriteString(strings.Join(values, "\t") + "\n")
}

func nest(reports map[string]fields, names []string) {
	rows := map[string]fields{}
	hosts := map[string]string{}
	var keys []string
	for _, n := range names {
		raw, _ := reports[n]["jobs"].([]any)
		reports[n]["jobs"] = []fields{}
		for _, v := range raw {
			r := v.(map[string]any)
			k := fmt.Sprint(n, ":", r["project"], "/", r["job"])
			keys = append(keys, k)
			rows[k] = r
			hosts[k] = n
		}
	}
	var roots []string
	children := map[string][]string{}
	for _, k := range keys {
		p, _ := rows[k]["parent"].(string)
		if !strings.Contains(p, ":") {
			if !strings.Contains(p, "/") {
				p = fmt.Sprint(rows[k]["project"], "/", p)
			}
			p = hosts[k] + ":" + p
		}
		if rows[p] != nil && rows[p]["session"] == rows[k]["session"] {
			children[p] = append(children[p], k)
		} else {
			roots = append(roots, k)
		}
	}
	var visit func(string, int)
	visit = func(k string, d int) {
		r := rows[k]
		if _, ok := r["depth"]; ok {
			return
		}
		r["depth"] = d
		n := hosts[k]
		reports[n]["jobs"] = append(reports[n]["jobs"].([]fields), r)
		for _, c := range children[k] {
			visit(c, d+1)
		}
	}
	for _, k := range append(roots, keys...) {
		visit(k, 0)
	}
	for _, host := range names {
		jobs := reports[host]["jobs"].([]fields)
		sort.SliceStable(jobs, func(i, j int) bool {
			a, _ := jobs[i]["session"].(string)
			b, _ := jobs[j]["session"].(string)
			return a < b
		})
	}
}
func decodeRows(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return dec.Decode(v)
}
