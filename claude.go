package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/djylb/timeshim"
)

type claudeEvent struct {
	Usage         *claudeUsage     `json:"usage"`
	APIStatus     *int             `json:"api_error_status"`
	RateLimitInfo *claudeRateLimit `json:"rate_limit_info"`
	Type, Result  string
	Subtype       string `json:"subtype"`
	Session       string `json:"session_id"`
	Message       claudeMessage
	Failed        bool `json:"is_error"`
}

// claudeMessage is the assistant/user message of a stream-json event. Some event
// types (for example system notices) carry message as a plain string; those have no
// content blocks, so any non-object value decodes to an empty message.
type claudeMessage struct {
	Content []struct{ Type, Text, Name string }
}

func (m *claudeMessage) UnmarshalJSON(b []byte) error {
	if t := bytes.TrimSpace(b); len(t) == 0 || t[0] != '{' {
		*m = claudeMessage{}
		return nil
	}
	type plain claudeMessage
	return json.Unmarshal(b, (*plain)(m))
}

type claudeUsage struct {
	Input         int64 `json:"input_tokens"`
	CacheCreation int64 `json:"cache_creation_input_tokens"`
	CacheRead     int64 `json:"cache_read_input_tokens"`
	Output        int64 `json:"output_tokens"`
}

// claudeRateLimit is the rate_limit_event payload of the stream-json output.
type claudeRateLimit struct {
	ResetsAt    *int64   `json:"resetsAt"`
	Utilization *float64 `json:"utilization"`
	Status      string   `json:"status"`
	Type        string   `json:"rateLimitType"`
}

// tokenCounts is the engine-neutral token usage shape stored in status.usage.
type tokenCounts struct {
	TotalTokens       int64 `json:"totalTokens"`
	InputTokens       int64 `json:"inputTokens"`
	CachedInputTokens int64 `json:"cachedInputTokens"`
	OutputTokens      int64 `json:"outputTokens"`
}

func (u claudeUsage) counts() tokenCounts {
	in := u.Input + u.CacheCreation + u.CacheRead
	return tokenCounts{TotalTokens: in + u.Output, InputTokens: in, CachedInputTokens: u.CacheRead, OutputTokens: u.Output}
}

// addUsage records one Claude turn as last and accumulates it into the previous total.
func addUsage(previous json.RawMessage, turn tokenCounts) (json.RawMessage, error) {
	var u struct{ Total tokenCounts }
	if len(previous) > 0 {
		_ = json.Unmarshal(previous, &u)
	}
	t := u.Total
	t.TotalTokens += turn.TotalTokens
	t.InputTokens += turn.InputTokens
	t.CachedInputTokens += turn.CachedInputTokens
	t.OutputTokens += turn.OutputTokens
	return json.Marshal(fields{"total": t, "last": turn})
}

// claudeRateLimited recognises a usage or rate limit failure in a Claude result.
func claudeRateLimited(e claudeEvent) bool {
	if e.APIStatus != nil && *e.APIStatus == 429 {
		return true
	}
	text := strings.ToLower(e.Result)
	for _, marker := range []string{"usage limit", "rate limit", "limit reached", "limit will reset", "resets at", "out of extra usage"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func (l claudeRateLimit) snapshot() rateLimitSnapshot {
	s := rateLimitSnapshot{LimitID: "claude"}
	switch {
	case l.Status == "rejected":
		s.RateLimitReachedType = l.Type
		if s.RateLimitReachedType == "" {
			s.RateLimitReachedType = "rejected"
		}
		s.Primary = &rateLimitReading{ResetsAt: l.ResetsAt, UsedPercent: 100}
	case l.Utilization != nil:
		pct := *l.Utilization
		if pct <= 1 {
			pct *= 100
		}
		s.Primary = &rateLimitReading{ResetsAt: l.ResetsAt, UsedPercent: int(math.Round(pct))}
	}
	return s
}

type claudeFrame struct {
	err   error
	event claudeEvent
	end   bool
}

// The exit grace applies only to a blocked pipe read. Decoding can spend
// arbitrary time waiting for the runner to consume already buffered events.
type claudeOutput struct {
	file    *os.File
	timer   timeshim.Timer
	clock   timeshim.Clock
	grace   time.Duration
	mu      sync.Mutex
	reading bool
	exited  bool
	expired bool
}

func (p *claudeOutput) arm() {
	var timer timeshim.Timer
	timer = timeshim.Or(p.clock).AfterFunc(p.grace, func() {
		p.mu.Lock()
		current := p.timer == timer && p.reading
		if current {
			p.timer = nil
			p.expired = true
		}
		p.mu.Unlock()
		if current {
			closeBestEffort(p.file)
		}
	})
	p.timer = timer
}

func (p *claudeOutput) childExited() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.exited = true
	if p.reading {
		p.arm()
	}
}

func (p *claudeOutput) Read(buf []byte) (int, error) {
	p.mu.Lock()
	if p.expired {
		p.mu.Unlock()
		return 0, io.EOF
	}
	p.reading = true
	if p.exited {
		p.arm()
	}
	p.mu.Unlock()
	n, err := p.file.Read(buf)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reading = false
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
	if p.expired && errors.Is(err, os.ErrClosed) {
		err = io.EOF
	}
	return n, err
}

func decodeClaudeOutput(reader io.Reader, send func(claudeFrame) bool) error {
	dec := json.NewDecoder(reader)
	for {
		var ev claudeEvent
		if err := dec.Decode(&ev); err != nil {
			return err
		}
		if !send(claudeFrame{event: ev}) {
			return nil
		}
	}
}

type claudeSession struct {
	cmd         *exec.Cmd
	stdout      *os.File
	exited      <-chan struct{}
	frames      chan claudeFrame
	stop        chan struct{}
	done        chan struct{}
	result      *claudeEvent
	interrupted bool
}

func (s *claudeSession) interrupt() {
	if s.cmd != nil {
		s.interrupted = true
		select {
		case <-s.exited:
		default:
			killTree(s.cmd)
		}
	}
}

func (s *claudeSession) join() error {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-s.done:
		return nil
	case <-timer.C:
		return errReapDeadline
	}
}

func (s *claudeSession) shutdown() error {
	if s.cmd == nil {
		return nil
	}
	s.interrupt()
	close(s.stop)
	closeBestEffort(s.stdout)
	return s.join()
}

// claudeArgs maps the job's sandbox, network, roots, effort and tools onto Claude CLI flags.
// mcpConfig names the job-tool server configuration, or is empty without job tools.
func (c *config) claudeArgs(session, mcpConfig string) []string {
	args := []string{"-p", "--output-format", "stream-json", "--input-format", "stream-json", "--verbose"}
	if session != "" {
		args = append(args, "--resume", session)
	}
	if c.Model != "" {
		args = append(args, "--model", c.Model)
	}
	if c.Effort != "" {
		args = append(args, "--effort", c.Effort)
	}
	tools := []string{"Read", "Glob", "Grep"}
	allowed := slices.Clone(tools)
	if c.Sandbox == "workspace-write" || c.Sandbox == dangerFullAccess {
		tools = append(tools, "Edit", "Write", "Bash")
		allowed = append(allowed, "Edit(./**)", "Write(./**)", "Bash")
		for _, root := range c.Roots {
			args = append(args, "--add-dir", root)
			pattern := "//" + strings.TrimPrefix(filepath.ToSlash(root), "/") + "/**"
			allowed = append(allowed, "Edit("+pattern+")", "Write("+pattern+")")
		}
	}
	if c.Network || c.WebSearch {
		tools = append(tools, "WebFetch")
		allowed = append(allowed, "WebFetch")
	}
	if c.WebSearch {
		tools = append(tools, "WebSearch")
		allowed = append(allowed, "WebSearch")
	}
	if mcpConfig != "" {
		args = append(args, "--mcp-config", mcpConfig)
		allowed = append(allowed, "mcp__"+jobToolServerName)
	}
	if c.Sandbox == dangerFullAccess {
		args = append(args, "--permission-mode", "bypassPermissions")
	} else {
		args = append(args, "--restricted", "--strict-mcp-config", "--permission-mode", "dontAsk", "--tools", strings.Join(tools, ","))
	}
	return append(args, "--allowedTools", strings.Join(allowed, ","))
}

// writeClaudeToolConfig writes the job-tool MCP server definition for --mcp-config.
func (r *runner) writeClaudeToolConfig() (string, error) {
	exe, args, err := jobToolServer(r.c, r.root, r.dir)
	if err != nil {
		return "", err
	}

	cfg := fields{"mcpServers": fields{jobToolServerName: fields{"type": "stdio", "command": exe, "args": args}}}
	data, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	path := filepath.Join(r.dir, "blobs", snapSum(data)+".json")
	return path, snapImmutable(path, data)
}

// Claude API image limits. cx reports violations instead of resizing or recompressing.
const (
	claudeMaxImageBytes  = 5 * 1024 * 1024
	claudeMaxImageSide   = 8000
	claudeMaxImagesTotal = 100
)

// claudeUserMessage converts cx input parts into one stream-json user message.
func claudeUserMessage(input any) ([]byte, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	var parts []json.RawMessage
	if err = json.Unmarshal(raw, &parts); err != nil {
		return nil, err
	}
	content, err := claudeContent(parts)
	if err != nil {
		return nil, err
	}
	return json.Marshal(fields{"type": "user", "message": fields{"role": "user", "content": content}})
}

// claudeContent maps cx input parts to Claude content blocks:
// text → text; localImage{path} → base64 image; image{url} → base64 (data: URL) or url source.
func claudeContent(parts []json.RawMessage) ([]fields, error) {
	var content []fields
	images := 0
	for _, raw := range parts {
		var p struct{ Type, Text, Path, URL string }
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, err
		}
		switch p.Type {
		case "text":
			content = append(content, fields{"type": "text", "text": p.Text})
			continue
		case "localImage":
			data, err := readSharedFile(p.Path)
			if err != nil {
				return nil, err
			}
			block, err := claudeImageBlock(filepath.Base(p.Path), data)
			if err != nil {
				return nil, err
			}
			content = append(content, block)
		case "image":
			block, err := claudeURLImage(p.URL)
			if err != nil {
				return nil, err
			}
			content = append(content, block)
		default:
			return nil, fmt.Errorf("input type %q not supported by engine claude", p.Type)
		}
		if images++; images > claudeMaxImagesTotal {
			return nil, fmt.Errorf("more than %d images in one message exceeds the Claude API limit", claudeMaxImagesTotal)
		}
	}
	if len(content) == 0 {
		return nil, errors.New("empty input")
	}
	return content, nil
}

func claudeImageBlock(name string, data []byte) (fields, error) {
	if len(data) > claudeMaxImageBytes {
		return nil, fmt.Errorf("image %s is %d bytes; the Claude API limit is %d bytes (5 MB)", name, len(data), claudeMaxImageBytes)
	}
	media, err := sniffImage(data)
	if err != nil {
		return nil, fmt.Errorf("image %s: %w", name, err)
	}
	if w, h, ok := imageSize(data); ok && (w > claudeMaxImageSide || h > claudeMaxImageSide) {
		return nil, fmt.Errorf("image %s is %dx%d pixels; the Claude API limit is %d pixels per side", name, w, h, claudeMaxImageSide)
	}
	return fields{"type": "image", "source": fields{"type": "base64", "media_type": media, "data": base64.StdEncoding.EncodeToString(data)}}, nil
}

func claudeURLImage(url string) (fields, error) {
	if rest, ok := strings.CutPrefix(url, "data:"); ok {
		meta, payload, found := strings.Cut(rest, ",")
		if !found || !strings.HasSuffix(meta, ";base64") {
			return nil, errors.New("image data URL must be base64 encoded")
		}
		data, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return nil, fmt.Errorf("image data URL: %w", err)
		}
		return claudeImageBlock("data URL", data)
	}
	if strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "http://") {
		return fields{"type": "image", "source": fields{"type": "url", "url": url}}, nil
	}
	return nil, fmt.Errorf("unsupported image URL %q", url)
}

func (r *runner) startClaude(input any, id string) error {
	data, err := claudeUserMessage(input)
	if err != nil {
		return err
	}
	if err = r.acquireSlot(); err != nil {
		return err
	}
	var toolConfig string
	if r.c.JobTools {
		if toolConfig, err = r.writeClaudeToolConfig(); err != nil {
			return err
		}
	}
	cmd := exec.Command("claude", r.c.claudeArgs(r.s.Thread, toolConfig)...)
	cmd.WaitDelay = 2 * time.Second
	cmd.Env = append(cmd.Environ(), fmt.Sprintf("MCP_TOOL_TIMEOUT=%d", jobToolTimeout.Milliseconds()))
	cmd.Dir = r.c.Dir
	cmd.Stdin = bytes.NewReader(append(data, '\n'))
	r.c.mark(cmd, r.dir)
	log, err := openJobFile(filepath.Join(r.dir, "server.stderr.log"), jobFileAppendCreate)
	if err != nil {
		return err
	}
	stderr, flush := r.c.secret.output(cmd, log)
	cmd.Stderr = stderr
	out, outWriter, err := os.Pipe()
	if err != nil {
		_ = log.Close()
		return err
	}
	cmd.Stdout = outWriter
	tree, err := r.c.startCachedCommand(cmd)
	_ = outWriter.Close()
	if err != nil {
		_ = out.Close()
		_ = log.Close()
		return err
	}
	s := r.claude
	s.cmd, s.stdout = cmd, out
	frames := make(chan claudeFrame, 8)
	stop, done := make(chan struct{}), make(chan struct{})
	s.frames, s.stop, s.done = frames, stop, done
	s.interrupted = false
	s.result = nil
	output := &claudeOutput{file: out, grace: 2 * time.Second}
	waited := make(chan error, 1)
	exited := make(chan struct{})
	s.exited = exited
	go func() {
		waitErr := cmd.Wait()
		close(exited)
		output.childExited()
		waited <- waitErr
	}()
	go func() {
		defer close(done)
		defer tree.cleanup()
		defer closeBestEffort(out)
		send := func(f claudeFrame) bool {
			select {
			case frames <- f:
				return true
			case <-stop:
				return false
			}
		}
		streamErr := decodeClaudeOutput(output, send)
		if streamErr != nil && !errors.Is(streamErr, io.EOF) {
			select {
			case <-exited:
			default:
				killTree(cmd)
			}
		}
		waitErr := <-waited
		if errors.Is(waitErr, exec.ErrWaitDelay) {
			waitErr = nil
		}
		waitErr = errors.Join(waitErr, flush(), log.Close())
		if errors.Is(streamErr, io.EOF) {
			streamErr = nil
		}
		send(claudeFrame{end: true, err: errors.Join(streamErr, waitErr)})
	}()
	r.s.EnginePID = cmd.Process.Pid
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
		if err := s.join(); err != nil {
			return err
		}
		s.cmd = nil
		s.frames = nil
		r.s.EnginePID = 0
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
			failure := fields{"message": result}
			if s.result != nil && claudeRateLimited(*s.result) {
				failure["rateLimited"] = true
			}
			t["error"] = failure
		}
		if s.result != nil && s.result.Usage != nil {
			usage, err := addUsage(r.s.Usage, s.result.Usage.counts())
			if err != nil {
				return err
			}
			r.s.Usage = usage
		}
		return r.claudeEvent("turn/completed", fields{"turn": t})
	}
	e := f.event
	r.s.LastEvent = timeshim.Or(r.clock).Now().UTC()
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
			switch block.Type {
			case "text":
				if err := r.claudeEvent("item/completed", fields{"item": fields{"type": "agentMessage", "text": block.Text}}); err != nil {
					return err
				}
			case "tool_use":
				if err := r.localEvent("claude/tool", r.c.secret.text(block.Name)); err != nil {
					return err
				}
			}
		}
	case "result":
		s.result = &e
	case "rate_limit_event":
		if e.RateLimitInfo != nil {
			r.s.update(e.RateLimitInfo.snapshot(), time.Now().Unix())
		}
	}
	return nil
}
func (r *runner) serveClaude() (err error) {
	s := &claudeSession{}
	r.claude = s
	defer func() {
		err = errors.Join(err, s.shutdown())
		if !r.closing.IsZero() {
			err = errors.Join(err, r.reclaim())
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
		r.idleSince = timeshim.Or(r.clock).Now()
		if err := r.save(); err != nil {
			return err
		}
	}
	tick := time.NewTicker(statePollInterval)
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
