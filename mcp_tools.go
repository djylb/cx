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
	"strings"
	"time"
)

func toolList() []fields {
	str := fields{"type": "string"}
	tool := func(name, desc string, props fields, required ...string) fields {
		schema := fields{"type": "object", "properties": props, "additionalProperties": false}
		if len(required) > 0 {
			schema["required"] = required
		}
		return fields{"name": name, "description": desc, "inputSchema": schema}
	}
	return []fields{
		tool("ask", "Ask the parent. A blocking request waits for an answer; timeout is in seconds.", fields{"question": str, "options": fields{"type": "array", "items": str}, "blocking": fields{"type": "boolean", "default": false}, "timeout": fields{"type": "number", "exclusiveMinimum": 0}}, "question"),
		tool("send", "Post a message for the parent, project/job, or #channel.", fields{"to": str, "text": str}, "to", "text"),
		tool("inbox", "Read messages delivered to this job, indexed by immutable message name.", fields{}),
	}
}
func callTool(ctx context.Context, dir string, params json.RawMessage) (any, error) {
	var p struct {
		Name      string
		Arguments json.RawMessage
	}
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	var a struct {
		Timeout            *float64
		Question, To, Text string
		Options            []string
		Blocking           bool
	}
	if p.Arguments != nil {
		if err := json.Unmarshal(p.Arguments, &a); err != nil {
			return nil, err
		}
	}
	m := agentMail{Type: p.Name}
	switch p.Name {
	case "inbox":
		return agentInbox(dir)
	case "ask":
		if strings.TrimSpace(a.Question) == "" {
			return nil, errors.New("question required")
		}
		m.To = "parent"
		m.Question = a.Question
		m.Options = a.Options
		m.Blocking = a.Blocking
		limit := defaultApproval
		if a.Timeout != nil {
			if *a.Timeout <= 0 || *a.Timeout >= float64(math.MaxInt64)/float64(time.Second) {
				return nil, errors.New("invalid timeout")
			}
			limit = time.Duration(*a.Timeout * float64(time.Second))
		}
		if m.Blocking {
			deadline := time.Now().UTC().Add(limit)
			m.Deadline = &deadline
		}
	case "send":
		if a.To != "parent" {
			if channel, ok := strings.CutPrefix(a.To, "#"); ok {
				if !jobName.MatchString(channel) {
					return nil, errors.New("invalid channel")
				}
			} else {
				proj, job, ok := strings.Cut(a.To, "/")
				if !ok {
					return nil, errors.New("expected project/job")
				}
				if _, err := jobPath("", proj); err != nil {
					return nil, err
				}
				if _, err := jobPath("", job); err != nil {
					return nil, err
				}
			}
		}
		if strings.TrimSpace(a.Text) == "" {
			return nil, errors.New("text required")
		}
		m.To = a.To
		m.Text = a.Text
	default:
		return nil, fmt.Errorf("unknown tool %q", p.Name)
	}
	if err := appendMail(ctx, dir, &m); err != nil {
		return nil, err
	}
	if !m.Blocking {
		return m, nil
	}
	id, _ := json.Marshal(m.Request)
	path, err := answerPath(dir, id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(ctx, *m.Deadline)
	defer cancel()
	for {
		var reply fields
		err := readJSON(path, &reply)
		if err == nil {
			return reply, nil
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
		if err = pause(ctx, 50*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

func serveTools(ctx context.Context, dir string, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 4096), 8<<20)
	enc := json.NewEncoder(out)
	for scanner.Scan() {
		var req message
		response := fields{"jsonrpc": "2.0", "id": nil}
		fail := func(code int, msg string) { response["error"] = fields{"code": code, "message": msg} }
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			fail(-32700, "parse error")
		} else if req.ID == nil {
			continue
		} else {
			response["id"] = req.ID
			switch req.Method {
			case "initialize":
				var init struct{ ProtocolVersion string }
				_ = json.Unmarshal(req.Params, &init)
				version := "2025-03-26"
				switch init.ProtocolVersion {
				case "2024-11-05", "2025-06-18", "2025-11-25":
					version = init.ProtocolVersion
				}
				response["result"] = fields{"protocolVersion": version, "capabilities": fields{"tools": fields{}}, "serverInfo": fields{"name": "cxrun", "version": "1"}}
			case "ping":
				response["result"] = fields{}
			case "tools/list":
				response["result"] = fields{"tools": toolList()}
			case "tools/call":
				v, err := callTool(ctx, dir, req.Params)
				var text string
				if err != nil {
					text = err.Error()
				} else {
					b, encodeErr := json.Marshal(v)
					if encodeErr != nil {
						return encodeErr
					}
					text = string(b)
				}
				response["result"] = fields{"content": []fields{{"type": "text", "text": text}}, "isError": err != nil}
			default:
				fail(-32601, "method not found")
			}
		}
		if err := enc.Encode(response); err != nil {
			return err
		}
	}
	return scanner.Err()
}
