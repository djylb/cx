package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type claudeEvent struct {
	Type, Result string
	Session      string `json:"session_id"`
	Failed       bool   `json:"is_error"`
	Message      struct {
		Content []struct{ Type, Text, Name string }
	}
}
type claudeFrame struct {
	event claudeEvent
	end   bool
	err   error
}
type claudeSession struct {
	cmd         *exec.Cmd
	frames      chan claudeFrame
	stop        chan struct{}
	done        chan struct{}
	interrupted bool
	result      *claudeEvent
}

func (s *claudeSession) interrupt() {
	if s.cmd != nil {
		s.interrupted = true
		killTree(s.cmd)
	}
}
func (c config) claudeArgs(session string) []string {
	args := []string{"-p", "--output-format", "stream-json", "--input-format", "stream-json", "--verbose"}
	if session != "" {
		args = append(args, "--resume", session)
	}
	if c.Model != "" {
		args = append(args, "--model", c.Model)
	}
	allowed := []string{"Read", "Glob", "Grep"}
	tools := []string{"Read", "Glob", "Grep"}
	if c.Sandbox == dangerFullAccess {
		args = append(args, "--permission-mode", "bypassPermissions")
	} else {
		args = append(args, "--restricted", "--strict-mcp-config", "--permission-mode", "dontAsk")
		if c.Sandbox == "workspace-write" {
			allowed = append(allowed, "Edit(./**)", "Write(./**)")
			tools = append(tools, "Edit", "Write")
		}
	}
	if c.WebSearch {
		allowed = append(allowed, "WebSearch", "WebFetch")
		tools = append(tools, "WebSearch", "WebFetch")
	}
	if c.Sandbox != dangerFullAccess {
		args = append(args, "--tools", strings.Join(tools, ","))
	}
	return append(args, "--allowedTools", strings.Join(allowed, ","))
}
func (r *runner) startClaude(input any, id string) error {
	raw, err := json.Marshal(input)
	if err != nil {
		return err
	}
	var parts []struct{ Type, Text string }
	if err = json.Unmarshal(raw, &parts); err != nil {
		return err
	}
	var lines []string
	for _, p := range parts {
		if p.Type != "text" {
			return fmt.Errorf("unsupported Claude input type %q", p.Type)
		}
		lines = append(lines, p.Text)
	}
	if err = r.acquireSlot(); err != nil {
		return err
	}
	data, err := json.Marshal(fields{"type": "user", "message": fields{"role": "user", "content": strings.Join(lines, "\n")}})
	if err != nil {
		return err
	}
	cmd := exec.Command("claude", r.c.claudeArgs(r.s.Thread)...)
	cmd.Dir = r.c.Dir
	cmd.Stdin = bytes.NewReader(append(data, '\n'))
	r.c.mark(cmd, r.dir)
	log, err := os.OpenFile(filepath.Join(r.dir, "server.stderr.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	stderr, flush := r.c.secret.output(cmd, log)
	cmd.Stderr = stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		log.Close()
		return err
	}
	tree, err := startCommand(cmd, r.c.RunID)
	if err != nil {
		out.Close()
		log.Close()
		return err
	}
	s := r.claude
	s.cmd = cmd
	s.frames = make(chan claudeFrame, 8)
	s.stop = make(chan struct{})
	s.done = make(chan struct{})
	s.interrupted = false
	s.result = nil
	go func() {
		defer close(s.done)
		defer tree.cleanup()
		send := func(f claudeFrame) bool {
			select {
			case s.frames <- f:
				return true
			case <-s.stop:
				return false
			}
		}
		dec := json.NewDecoder(out)
		var streamErr error
		for {
			var raw json.RawMessage
			streamErr = dec.Decode(&raw)
			if streamErr == nil {
				raw, streamErr = r.c.secret.jsonStrings(raw)
			}
			var ev claudeEvent
			if streamErr == nil {
				streamErr = json.Unmarshal(raw, &ev)
			}
			if streamErr != nil {
				break
			}
			if !send(claudeFrame{event: ev}) {
				break
			}
		}
		if streamErr != nil && !errors.Is(streamErr, io.EOF) {
			killTree(cmd)
		}
		waitErr := cmd.Wait()
		if errors.Is(waitErr, exec.ErrWaitDelay) {
			waitErr = nil
		}
		waitErr = errors.Join(waitErr, flush(), log.Close())
		if errors.Is(streamErr, io.EOF) {
			streamErr = nil
		}
		send(claudeFrame{end: true, err: errors.Join(streamErr, waitErr)})
	}()
	r.s.ServerPID = cmd.Process.Pid
	turnID := fmt.Sprintf("turn-%d", time.Now().UnixNano())
	if err = r.localEvent("turn/start", id+" → "+turnID); err != nil {
		return err
	}
	return r.begin(turnID)
}
func (r *runner) claudeEvent(method string, params fields) error {
	params["threadId"] = r.s.Thread
	params["turnId"] = r.s.Turn
	raw, err := json.Marshal(fields{"method": method, "params": params})
	if err != nil {
		return err
	}
	_, err = r.read(frame{raw: raw})
	return err
}
func (r *runner) consumeClaude(f claudeFrame) error {
	s := r.claude
	if f.end {
		<-s.done
		s.cmd = nil
		s.frames = nil
		r.s.ServerPID = 0
		status := "completed"
		result := ""
		if s.result != nil {
			result = s.result.Result
		}
		switch {
		case s.interrupted:
			status = "interrupted"
		case f.err != nil || s.result == nil || s.result.Failed:
			status = "failed"
			if result == "" {
				result = fmt.Sprintf("Claude exited without a successful result (%v); see server.stderr.log", f.err)
			}
		}
		t := fields{"id": r.s.Turn, "status": status, "items": []fields{{"type": "agentMessage", "text": result}}}
		if status == "failed" {
			t["error"] = fields{"message": result}
		}
		return r.claudeEvent("turn/completed", fields{"turn": t})
	}
	e := f.event
	r.s.LastEvent = time.Now().UTC()
	r.s.StallCount = 0
	r.dirty = true
	if e.Session != "" && e.Session != r.s.Thread {
		if r.s.Thread != "" {
			return errors.New("unexpected session ID from Claude")
		}
		r.s.Thread = e.Session
		if err := r.save(); err != nil {
			return err
		}
	}
	switch e.Type {
	case "assistant":
		for _, block := range e.Message.Content {
			if block.Type == "text" {
				if err := r.claudeEvent("item/completed", fields{"item": fields{"type": "agentMessage", "text": block.Text}}); err != nil {
					return err
				}
			} else if block.Type == "tool_use" {
				if err := r.localEvent("claude/tool", block.Name); err != nil {
					return err
				}
			}
		}
	case "result":
		s.result = &e
	}
	return nil
}
func (r *runner) serveClaude() error {
	s := &claudeSession{}
	r.claude = s
	defer func() {
		if s.cmd != nil {
			s.interrupt()
			close(s.stop)
			<-s.done
		}
	}()
	stop := r.startRuntime()
	defer stop()
	r.s.Generation++
	if err := r.recoverRPC(); err != nil {
		return err
	}
	if err := r.recoverInbox(turn{}); err != nil {
		return err
	}
	if r.active() {
		if err := r.finish(turn{ID: r.s.Turn, Status: "interrupted"}); err != nil {
			return err
		}
	}
	if r.s.Finished == 0 {
		if err := r.input(r.c.Prompt, "initial", false); err != nil {
			return err
		}
	} else {
		r.releaseSlot()
		r.deriveState()
		r.idleSince = time.Now()
		if err := r.save(); err != nil {
			return err
		}
	}
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case f := <-s.frames:
			if err := r.consumeClaude(f); err != nil {
				return err
			}
		case <-tick.C:
			done, err := r.pollAgent()
			if done || err != nil {
				return err
			}
		}
	}
}
