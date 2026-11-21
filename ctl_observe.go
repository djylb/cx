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
	"sync"
	"time"
)

type hostReply struct {
	name string
	r    reply
	e    error
}

func displayHosts(ctx context.Context, hs endpoints, op string, o ctlOptions) error {
	names := hs.names()
	filter := ""
	if op == "doctor" && len(o.pos) > 0 {
		names = o.pos
	} else if op == "ls" && len(o.pos) == 1 {
		host, id, ok := strings.Cut(o.pos[0], ":")
		if !ok {
			return errors.New("expected HOST:project/job")
		}
		names = []string{host}
		filter = id
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
						ch <- hostReply{name, reply{}, err}
						return
					}
					h.setPlatform(plat)
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
				ch <- hostReply{name, r, e}
			}()
		}
		results := map[string]hostReply{}
		for range names {
			v := <-ch
			results[v.name] = v
		}
		reports := map[string]fields{}
		var output bytes.Buffer
		enc := json.NewEncoder(&output)
		failed := false
		current := map[string]string{}
		for _, name := range names {
			v := results[name]
			if v.e != nil || v.r.code != 0 {
				failed = true
				reports[name] = fields{"error": fmt.Sprintf("node error %v (code %d)", v.e, v.r.code), "jobs": []fields{}}
				if op == "ls" {
					fmt.Fprintln(os.Stderr, name, reports[name]["error"])
				}
				continue
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
			if e := decodeRows(v.r.data, &rows); e != nil {
				return e
			}
			for _, row := range rows {
				id := strings.TrimLeft(fmt.Sprint(row["project"], "/", row["job"]), "/")
				if filter != "" && id != filter {
					continue
				}
				key := name + ":" + id
				role := row["role"]
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
						fmt.Fprintln(&output, "JOB\tHOST\tROLE\tSTATUS\tTURNS\tPLAN\tFILES\tFAILED COMMAND\tRATE LIMIT\tTOKENS\tCONTEXT %")
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
					values := []any{id, name, role, state, row["completed_turns"], row["plan_step"], row["changed_files"], failure, string(rate), row["tokens"], row["context_percent"]}
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
					fmt.Fprintf(&output, "%s\t%s\t-\tremoved\n", id, name)
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
					fmt.Fprintf(&output, "%s %s\n", name, b)
					if rows, ok := jobs.([]fields); ok {
						for _, r := range rows {
							fmt.Fprintf(&output, "%s%s/%s %s %s %v\n", strings.Repeat("  ", 1+r["depth"].(int)), r["project"], r["job"], r["role"], r["status"], r["plan_step"])
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
		if e := pause(ctx, time.Second); e != nil {
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
func ctlWaitAny(ctx context.Context, hs endpoints, o ctlOptions) error {
	if len(o.pos) != 0 {
		return errors.New("wait --any takes no job")
	}
	limit, e := observation(o.timeout)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	if o.cursor == "" {
		o.cursor = "default"
	}
	parts := []string{"wait", o.cursor}
	if o.session != "" {
		parts = append(parts, "sessions", snapSum([]byte(o.session)))
	}
	path, e := cursorFile(append(parts, "cursor")...)
	if e != nil {
		return e
	}
	lock, e := lockUntil(ctx, path+".lock")
	if e != nil {
		return e
	}
	defer lock.Close()
	names := o.hosts
	if len(names) == 0 {
		names = hs.names()
	}
	observerCount := len(names) + len(o.channels)
	ch := make(chan hostReply, observerCount)
	var observers sync.WaitGroup
	defer func() {
		cancel()
		observers.Wait()
	}()
	start := func(name string, poll func() (reply, error)) {
		observers.Add(1)
		go func() {
			defer observers.Done()
			r, err := poll()
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || errors.Is(err, ctlExit(124)) {
				r, err = reply{code: 124}, nil
			}
			ch <- hostReply{name, r, err}
		}()
	}
	chatFiles := map[string]string{}
	for _, channel := range o.channels {
		p := o.project
		if p == "" {
			p, e = defaultProject(ctx)
			if e != nil {
				return e
			}
		}
		key := strings.TrimPrefix(channel, "#")
		if _, e := jobDir("", p+"/"+key); e != nil {
			return e
		}
		path := filepath.Join(filepath.Dir(path), "chat", p, key+".json")
		if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
			return e
		}
		var since int64
		if e = readJSON(path, &since); e != nil && !os.IsNotExist(e) {
			return e
		}
		h, root, e := channelCenter(hs)
		if e != nil {
			return e
		}
		label := "#" + key
		chatFiles[label] = path
		start(label, func() (reply, error) {
			q := channelQuery{Project: p, Channel: key, Root: root, Since: since, Wait: limit}
			rows, e := channelCall(ctx, h, q)
			if e != nil {
				return reply{}, e
			}
			if len(rows) == 0 {
				return reply{}, errors.New("missing channel event")
			}
			row := struct {
				channelRow
				Channel string `json:"channel"`
				Project string `json:"project"`
				Code    int    `json:"code"`
			}{rows[0], channel, p, 0}
			b, e := json.Marshal(fields{"row": row, "cursor": rows[0].Seq})
			return reply{data: b}, e
		})
	}

	for _, name := range names {
		h, e := hs.get(name)
		if e != nil {
			return e
		}
		file := filepath.Join(filepath.Dir(path), name+".json")
		b, e := os.ReadFile(file)
		if os.IsNotExist(e) {
			b = []byte("{}")
		} else if e != nil {
			return e
		}
		start(name, func() (reply, error) {
			for ctx.Err() == nil {
				end, _ := ctx.Deadline()
				d := time.Until(end)
				if d <= 0 {
					break
				}
				r, e := h.call(ctx, d+2*time.Second, "wait-poll", o.project, o.session, string(b), d.String())
				if e != nil || r.code != 124 {
					return r, e
				}
			}
			return reply{code: 124}, nil
		})
	}
	last := ctlExit(124)
	for range observerCount {
		v := <-ch
		if v.e != nil || v.r.code != 0 {
			if v.e != nil || v.r.code != 124 {
				last = ctlExit(9)
			}
			continue
		}
		var report struct {
			Row    fields
			Cursor json.RawMessage
		}
		if e = json.Unmarshal(v.r.data, &report); e != nil {
			return e
		}
		file := chatFiles[v.name]
		if file == "" {
			report.Row["host"] = v.name
			file = filepath.Join(filepath.Dir(path), v.name+".json")
		}
		if e = json.NewEncoder(os.Stdout).Encode(report.Row); e != nil {
			return e
		}
		if e = writeJSON(file, report.Cursor); e != nil {
			return e
		}
		code, _ := report.Row["code"].(float64)
		if code != 0 {
			return ctlExit(code)
		}
		return nil
	}
	return last
}

func decodeRows(b []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return dec.Decode(v)
}
