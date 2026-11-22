package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type channelRow struct {
	Author string `json:"author"`
	Role   string `json:"role"`
	Time   string `json:"time"`
	Type   string `json:"type"`
	Text   string `json:"text"`
	Seq    int64  `json:"seq"`
}
type channelQuery struct {
	Message                                *channelRow
	Project, Channel, Root, Token, Mention string
	Since                                  int64
	Wait                                   time.Duration
}

func channelCall(ctx context.Context, h endpoint, q channelQuery) ([]channelRow, error) {
	b, e := json.Marshal(q)
	if e != nil {
		return nil, e
	}
	r, e := h.call(ctx, 30*time.Second+q.Wait, "channel", string(b))
	if e != nil {
		return nil, e
	}
	if r.code != 0 {
		return nil, ctlExit(r.code)
	}
	var rows []channelRow
	e = json.Unmarshal(r.data, &rows)
	return rows, e
}
func ctlChat(ctx context.Context, hs endpoints, action string, o ctlOptions) error {
	p, e := defaultProject(ctx)
	if e != nil {
		return e
	}
	if _, e = jobDir("", p+"/channel"); e != nil {
		return e
	}
	h, root, e := channelCenter(hs)
	if e != nil {
		return e
	}
	var q channelQuery
	q.Project = p
	q.Root = root
	if len(o.pos) == 0 {
		return errors.New("chat requires channel")
	}
	q.Channel = strings.TrimPrefix(o.pos[0], "#")
	switch action {
	case "post", "forward":
		kind := o.kind
		if kind == "" {
			kind = "discussion"
		}
		author := os.Getenv("CX_SESSION")
		if author == "" {
			author = "controller"
		}
		role := os.Getenv("CX_ROLE")
		if role == "" {
			role = "manager"
		}
		var b [16]byte
		if _, e = rand.Read(b[:]); e != nil {
			return e
		}
		q.Token = fmt.Sprintf("%x", b)
		var text string
		if action == "post" {
			if len(o.pos) != 2 {
				return errors.New("chat post CH TEXT")
			}
			switch kind {
			case "report", "proposal", "discussion", "decision", "escalation":
			default:
				return errors.New("invalid message type")
			}
			text = o.pos[1]
			if strings.HasPrefix(text, "@") {
				text, e = readInputText(text[1:])
				if e != nil {
					return e
				}
			}
		} else {
			if len(o.pos) != 3 {
				return errors.New("chat forward JOB SEQ CH")
			}
			q.Channel = strings.TrimPrefix(o.pos[2], "#")
			name, id, forwardErr := resolveJob(ctx, o.pos[0])
			if forwardErr != nil {
				return forwardErr
			}
			if !strings.HasPrefix(id, p+"/") {
				return errors.New("job belongs to another project")
			}
			seq, forwardErr := strconv.ParseUint(o.pos[1], 10, 64)
			if forwardErr != nil || seq == 0 {
				return errors.New("invalid sequence")
			}
			node, forwardErr := hs.get(name)
			if forwardErr != nil {
				return forwardErr
			}
			r, forwardErr := node.call(ctx, 30*time.Second, "forward-message", id, fmt.Sprint(seq))
			if forwardErr != nil {
				return forwardErr
			}
			if r.code != 0 {
				return r.output()
			}
			var msg struct {
				Role, Generation string
				Message          agentMail
			}
			if forwardErr = json.Unmarshal(r.data, &msg); forwardErr != nil {
				return forwardErr
			}
			if msg.Message.Type != "send" || msg.Message.To != "#"+q.Channel {
				return errors.New("message does not target channel")
			}
			text, author, role, kind = msg.Message.Text, id, msg.Role, "report"
			payload, _ := json.Marshal([]any{name, id, msg.Generation, seq})
			q.Token = snapSum(payload)
		}
		q.Message = &channelRow{author, role, time.Now().UTC().Format(time.RFC3339Nano), kind, text, 0}
	case "wait", "read":
		if len(o.pos) != 1 || o.since != "" && o.cursor != "" {
			return errors.New("chat requires CH and one cursor or offset")
		}
		if o.since != "" {
			q.Since, e = strconv.ParseInt(o.since, 10, 64)
			if e != nil || q.Since < 0 {
				return errors.New("invalid chat offset")
			}
		}
		if action == "wait" {
			q.Wait, e = observation(o.timeout)
			if e != nil {
				return e
			}
			q.Mention = o.mention
			if o.cursor == "" && o.since == "" {
				o.cursor = "default"
			}
		}
	default:
		return errors.New("unknown chat action")
	}
	if _, e = jobPath("", q.Channel); e != nil {
		return e
	}
	var cursor string
	if o.cursor != "" {
		cursor, e = channelCursorPath(p, q.Channel, o.cursor)
		if e == nil {
			e = os.MkdirAll(filepath.Dir(cursor), 0700)
		}
		if e != nil {
			return e
		}
		l, cursorErr := lockUntil(ctx, cursor+".lock")
		if cursorErr != nil {
			return cursorErr
		}
		defer closeBestEffort(l)
		if cursorErr = readJSON(cursor, &q.Since); cursorErr != nil && !os.IsNotExist(cursorErr) {
			return cursorErr
		}
	}
	var rows []channelRow
	if runtime.GOOS == "darwin" && q.Message != nil && q.Message.Type == "decision" {
		rows, e = journalDecision(ctx, h, q)
	} else {
		rows, e = channelCall(ctx, h, q)
	}
	if e != nil {
		return e
	}
	enc := json.NewEncoder(os.Stdout)
	for _, r := range rows {
		if e = enc.Encode(r); e != nil {
			return e
		}
	}
	if cursor != "" && len(rows) > 0 {
		return writeJSON(cursor, rows[len(rows)-1].Seq)
	}
	return nil
}
func forwardMessage(id, number string) error {
	n, e := strconv.ParseUint(number, 10, 64)
	if e != nil || n == 0 {
		return errors.New("invalid message sequence")
	}
	root, e := stateRoot()
	if e != nil {
		return e
	}
	dir, e := jobDir(root, id)
	if e != nil {
		return e
	}
	var s status
	if e = loadJobStatusInto(dir, &s); e != nil {
		return e
	}
	work, e := workDir(dir, s)
	if e != nil {
		return e
	}
	var meta struct{ Role string }
	if e = readJSON(filepath.Join(work, "request.json"), &meta); e != nil && !os.IsNotExist(e) {
		return e
	}
	if meta.Role == "" {
		meta.Role = "worker"
	}
	var msg agentMail
	e = readMail(dir, n-1, func(m agentMail) error {
		if m.Seq == n {
			msg = m
		}
		return nil
	})
	if e != nil {
		return e
	}
	if msg.Seq != n {
		return errors.New("message unavailable")
	}
	return json.NewEncoder(os.Stdout).Encode(fields{"message": msg, "generation": s.RunID, "role": meta.Role})
}
func channelNode(ctx context.Context, raw string) error {
	var q channelQuery
	if e := json.Unmarshal([]byte(raw), &q); e != nil {
		return e
	}
	if _, e := jobDir("", q.Project+"/channel"); e != nil {
		return e
	}
	if _, e := jobPath("", q.Channel); e != nil {
		return e
	}
	if q.Root == "" {
		root, e := stateRoot()
		if e != nil {
			return e
		}
		q.Root = filepath.Join(root, "repos")
	}
	repo := filepath.Join(q.Root, q.Project+"-meta.git")
	ref := "refs/cx/chat/" + q.Channel
	g := snapGit{ctx: ctx, dir: repo, env: []string{"GIT_AUTHOR_NAME=cx", "GIT_COMMITTER_NAME=cx", "GIT_AUTHOR_EMAIL=cx@invalid", "GIT_COMMITTER_EMAIL=cx@invalid"}}
	if q.Message != nil {
		if !snapHex(q.Token, 32) && !snapHex(q.Token, 64) {
			return errors.New("invalid chat token")
		}
		if e := os.MkdirAll(q.Root, 0700); e != nil {
			return e
		}
		lock, e := lockUntil(ctx, filepath.Join(q.Root, q.Project+".chat.lock"))
		if e != nil {
			return e
		}
		defer closeBestEffort(lock)
		if _, e = os.Stat(repo); os.IsNotExist(e) {
			c := exec.CommandContext(ctx, "git", "init", "--bare", "--quiet", repo)
			if e = c.Run(); e != nil {
				return e
			}
		} else if e != nil {
			return e
		}
	}
	var match *regexp.Regexp
	if q.Mention != "" {
		match = regexp.MustCompile("(^|[^[:alnum:]_])@" + regexp.QuoteMeta(q.Mention) + "($|[^[:alnum:]_.-])")
	}
	end := time.Now().Add(q.Wait)
	for {
		var head string
		if _, e := os.Stat(repo); e == nil {
			if _, e = g.get("check-ref-format", ref); e != nil {
				return e
			}
			head, e = g.get("rev-parse", "--verify", "--quiet", ref)
			if e != nil {
				var ex *exec.ExitError
				if !errors.As(e, &ex) || ex.ExitCode() != 1 {
					return e
				}
				head = ""
			}
		} else if !os.IsNotExist(e) {
			return e
		}
		var tip channelRow
		if head != "" {
			b, e := g.input(nil, "show", "-s", "--format=%s", head)
			if e != nil {
				return e
			}
			if e = json.Unmarshal(b, &tip); e != nil {
				return e
			}
		}
		if q.Message != nil {
			if head != "" {
				previous, e := g.input(nil, "log", "-1", "--grep=^CX-Message: "+q.Token+"$", "--format=%s", head)
				if e != nil {
					return e
				}
				if len(previous) > 0 {
					if e = json.Unmarshal(previous, &tip); e != nil {
						return e
					}
					return json.NewEncoder(os.Stdout).Encode([]channelRow{tip})
				}
			}
			row := *q.Message
			row.Seq = tip.Seq + 1
			tree, e := g.get("mktree")
			if e != nil {
				return e
			}
			args := []string{"commit-tree", tree}
			if head != "" {
				args = append(args, "-p", head)
			}
			b, e := json.Marshal(row)
			if e != nil {
				return e
			}
			oid, e := g.input(append(b, []byte("\n\nCX-Message: "+q.Token+"\n")...), args...)
			if e != nil {
				return e
			}
			commit := strings.TrimSpace(string(oid))
			if head == "" {
				head = strings.Repeat("0", len(commit))
			}
			if _, e = g.get("update-ref", ref, commit, head); e != nil {
				return e
			}
			return json.NewEncoder(os.Stdout).Encode([]channelRow{row})
		}
		var rows []channelRow
		if tip.Seq > q.Since {
			b, e := g.get("log", "--reverse", "-n", fmt.Sprint(tip.Seq-q.Since), "--format=%s", head)
			if e != nil {
				return e
			}
			for _, line := range strings.Split(b, "\n") {
				var row channelRow
				if e = json.Unmarshal([]byte(line), &row); e != nil {
					return e
				}
				q.Since = row.Seq
				if match == nil || match.MatchString(row.Text) {
					rows = append(rows, row)
					if q.Wait > 0 {
						break
					}
				}
			}
		}
		if len(rows) > 0 || q.Wait <= 0 {
			return json.NewEncoder(os.Stdout).Encode(rows)
		}
		if time.Now().After(end) {
			return ctlExit(124)
		}
		if e := pause(ctx, min(400*time.Millisecond, time.Until(end))); e != nil {
			return e
		}
	}
}

func channelCenter(hs endpoints) (endpoint, string, error) {
	host := defaultSubmitHost()
	root := os.Getenv("CX_CHAT_LOCAL_ROOT")
	if root != "" {
		host = "local"
	}
	h, e := hs.get(host)
	return h, root, e
}
