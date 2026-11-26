package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type hostReply struct {
	e    error
	name string
	r    reply
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
				s.State != "submitting" && s.State != "submit_failed" {
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

func displayHosts(ctx context.Context, hs endpoints, op string, o ctlOptions) error {
	names := hs.names()
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
					node := op
					if op == "ls" {
						node = "rows"
					}
					args = []string{node, o.project, o.session}
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
				ch <- hostReply{name: name, r: r, e: e}
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
		current := map[string]string{}
		for _, name := range names {
			v := results[name]
			nodeFailed := v.e != nil || v.r.code != 0
			if nodeFailed {
				failed = true
				reports[name] = fields{"error": fmt.Sprintf("node error %v (code %d)", v.e, v.r.code), "jobs": []fields{}}
				if op != "ls" {
					continue
				}
				diagnosticf("%v %v\n", name, reports[name]["error"])
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
				if op == "doctor" && unsupportedMaster.Load() {
					obj["controller_ssh_multiplexing"] = "unsupported; using ordinary connections"
				}
				reports[name] = obj
				continue
			}
			var rows []fields
			if !nodeFailed {
				if e := decodeRows(v.r.data, &rows); e != nil {
					return e
				}
			}
			rows, e := mergeSubmissionRows(ctx, name, filter, o, rows)
			if e != nil {
				return e
			}
			for _, row := range rows {
				id := listingJobID(row)
				if filter != "" && id != filter {
					continue
				}
				key := name + ":" + id
				role := row["role"]
				if o.role != "" && role != o.role {
					continue
				}
				delete(row, "role")
				priv := row["privileged"]
				delete(row, "privileged")
				if o.watch {
					fp, _ := json.Marshal([]any{row, role, priv})
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
					if header {
						output.WriteString("JOB\tHOST\tROLE\tSTATUS\tTURNS\tPLAN\tFILES\tFAILED COMMAND\tRATE LIMIT\tTOKENS\tCONTEXT %\tPROFILE\n")
						header = false
					}
					state := fmt.Sprint(row["status"])
					if state == "waiting_input" {
						state += " awaiting reply"
					}
					if priv == true {
						state += " privileged"
					}
					var failure any
					if v, ok := row["failed_command"].(map[string]any); ok {
						failure = fmt.Sprintf("%v (%v)", v["command"], v["exit_code"])
					}
					rate, _ := json.Marshal(row["rate_limit"])
					values := []any{id, name, role, state, row["completed_turns"], row["plan_step"], row["changed_files"], failure, string(rate), row["tokens"], row["context_percent"], row["profile"]}
					for i, v := range values {
						if i > 0 {
							output.WriteByte('\t')
						}
						if v == nil {
							output.WriteByte('-')
						} else {
							output.WriteString(strings.Join(strings.Fields(fmt.Sprint(v)), " "))
						}
					}
					output.WriteByte('\n')
				}
			}
		}
		if o.watch {
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
					_, _ = fmt.Fprintf(&output, "%s\t%s\t-\tremoved\n", id, name)
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
