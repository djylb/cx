package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type item struct {
	Type, ID, Text, ClientID, Command, Status string
	ExitCode                                  *int
	Changes                                   []struct {
		Path string
		Kind struct{ Type string }
	}
}
type turn struct {
	Error      *turnError
	ID, Status string
	Items      []item
}
type event struct {
	Error                               *turnError
	WillRetry, Retry                    *bool
	ThreadID, TurnID, Diff, Explanation string
	Item                                item
	RateLimits                          rateLimitSnapshot
	Plan                                []struct{ Step, Status string }
	TokenUsage, RequestID               json.RawMessage
	Turn                                turn
}

type turnError struct {
	Message           string
	AdditionalDetails string
	Code              json.RawMessage
	CodexErrorInfo    json.RawMessage
}

const failedReportHeading = "STATUS: FAILED"

func reportStatus(firstLine string) string {
	state, ok := strings.CutPrefix(strings.TrimSuffix(firstLine, "\r"), "STATUS: ")
	if ok {
		switch state {
		case "DONE", "PARTIAL", "BLOCKED", "QUESTION", "FAILED":
			return state
		}
	}
	return ""
}

func (r *runner) describeFailure(e *turnError) string {
	report := failedReportHeading + "\n"
	if e != nil {
		report += r.c.secret.text(e.Message) + "\n"
		if e.AdditionalDetails != "" {
			report += r.c.secret.text(e.AdditionalDetails) + "\n"
		}
	}
	report += "retry=false\n"
	if e != nil {
		if len(e.Code) != 0 && string(e.Code) != "null" {
			report += "code=" + r.c.secret.text(string(e.Code)) + "\n"
		}
		if len(e.CodexErrorInfo) != 0 && string(e.CodexErrorInfo) != "null" {
			report += "codexErrorInfo=" + r.c.secret.text(string(e.CodexErrorInfo)) + "\n"
		}
	}
	return report
}

type pendingTurnFailure struct {
	deadline time.Time
	failure  *turnError
	id       string
}

func (r *runner) flushTurnFailure() error {
	if f := r.turnFailure; f != nil {
		return r.finish(turn{ID: f.id, Error: f.failure})
	}
	return nil
}

func (r *runner) flushExpiredTurnFailure() error {
	if f := r.turnFailure; f != nil && !time.Now().Before(f.deadline) {
		return r.flushTurnFailure()
	}
	return nil
}

type failedCommand struct {
	Command  string `json:"command"`
	ExitCode int64  `json:"exit_code"`
}

type turnSummary struct {
	FailedCommand *failedCommand `json:"failed_command"`
	PlanStep      string         `json:"plan_step"`
	ChangedFiles  int            `json:"changed_files"`
}

func brief(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	rs := []rune(s)
	if len(rs) > 1200 {
		return string(rs[:1200]) + "…"
	}
	return s
}
func (r *runner) note(kind, s string) error {
	return r.writeProgress(kind, r.c.secret.text(s))
}

// Some progress records are read back to locate a turn. Their identity and
// status columns must agree with the status file and the unmodified protocol.
func (r *runner) writeProgress(kind, s string) error {
	fi, err := r.log.Stat()
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(r.log, "%d\t%s\t%s\t%s\n", fi.Size()+1, time.Now().UTC().Format(time.RFC3339Nano), kind, brief(s))
	return err
}
func (r *runner) begin(id string) error {
	if id == "" {
		return fmt.Errorf("missing turn id")
	}
	if r.s.Turn != id {
		r.s.PlanStep = ""
		r.s.ChangedFiles = 0
	}
	if r.s.Turn != id || !r.active() {
		r.lastText = ""
		r.turnSince = time.Now()
	}
	r.s.InputKind = ""
	r.s.Turn = id
	r.s.TurnStatus = "inProgress"
	r.deriveState()
	return r.save()
}
func (r *runner) finish(t turn) error {
	if t.ID == "" {
		return fmt.Errorf("missing completed turn id")
	}
	if t.Status != "" {
		if err := r.noteRateLimit(t.ID, "turn/completed", false, t.Error); err != nil {
			return err
		}
	}
	if f := r.turnFailure; f != nil && f.id == t.ID {
		t.Error = f.failure
		r.turnFailure = nil
	}
	if t.Error != nil {
		t.Status = "failed"
	}
	// A terminal error notification can precede turn/completed. The later
	// notification may supply the final agent item, but cannot undo failure.
	alreadyFailed := t.ID == r.s.LastCompleted && r.s.CompletedStatus == "failed"
	if alreadyFailed && (r.s.Turn != t.ID || r.failureReport == "") {
		return nil
	}
	if alreadyFailed {
		t.Status = "failed"
	}
	if t.Status != "interrupted" || r.pending == "" {
		r.releaseSlot()
	}
	r.cmdOpen = nil
	if t.Status == "interrupted" && r.pending != "" {
		r.s.TurnStatus = t.Status
		r.s.State = "running"
		return r.save()
	}
	r.pending = ""
	r.s.StallCount = 0
	if r.s.Turn != t.ID {
		r.s.PlanStep = ""
		r.s.ChangedFiles = 0
		r.lastText = ""
	}
	for _, i := range t.Items {
		if i.Type == "agentMessage" {
			r.lastText = i.Text
		}
	}
	report := r.c.secret.reportText(r.lastText)
	if t.Status == "failed" {
		r.s.Approvals = nil
		if !alreadyFailed {
			r.failureReport = r.describeFailure(t.Error)
		}
		if report != "" {
			report = "\n" + report
		}
		report = r.failureReport + report
	}
	if err := atomicWrite(filepath.Join(r.dir, "last.md"), []byte(report)); err != nil {
		return err
	}
	r.s.Turn = t.ID
	r.s.TurnStatus = t.Status
	if t.ID != r.s.LastCompleted {
		r.s.Finished++
		r.s.LastCompleted = t.ID
	}
	r.s.CompletedStatus = t.Status
	r.s.InputKind = ""
	line, _, _ := strings.Cut(r.lastText, "\n")
	if r.c.Interactive && t.Status == "completed" && reportStatus(line) == "QUESTION" {
		r.s.InputKind = "next_turn"
	}
	r.deriveState()
	r.idleSince = time.Now()
	info := t.ID + " " + t.Status
	if t.Error != nil {
		info += " " + r.c.secret.text(t.Error.Message)
	}
	if !alreadyFailed {
		if err := r.writeProgress("turn/completed", info); err != nil {
			return err
		}
	}
	return r.save()
}
func (r *runner) notify(m message) error {
	if m.Method == "item/agentMessage/delta" || m.Method == "item/commandExecution/outputDelta" {
		r.s.StallCount = 0
	}
	// Deltas remain in events.jsonl. Completed items carry full text.
	switch m.Method {
	case "turn/started", "turn/completed", "turn/plan/updated", "turn/diff/updated", "item/started", "item/completed", "thread/tokenUsage/updated", "account/rateLimits/updated", "error", "serverRequest/resolved":
	default:
		return nil
	}
	var e event
	if err := json.Unmarshal(m.Params, &e); err != nil {
		return err
	}
	if e.ThreadID != "" && e.ThreadID != r.s.Thread {
		return nil
	}
	if e.TurnID != "" && r.s.Turn != "" && e.TurnID != r.s.Turn {
		if m.Method != "error" || r.active() || r.slot == nil {
			return nil
		}
	}
	switch m.Method {
	case "turn/started":
		if e.Turn.ID != "" && e.Turn.ID == r.s.LastCompleted {
			return nil
		}
		if err := r.writeProgress(m.Method, e.Turn.ID); err != nil {
			return err
		}
		if e.Turn.Error != nil || e.Turn.Status == "failed" {
			return r.finish(e.Turn)
		}
		return r.begin(e.Turn.ID)
	case "turn/completed":
		return r.finish(e.Turn)
	case "thread/tokenUsage/updated":
		r.s.Usage = e.TokenUsage
	case "account/rateLimits/updated":
		r.s.update(e.RateLimits, time.Now().Unix())
	case "error":
		if e.Error != nil {
			retry := e.Retry != nil && *e.Retry
			if e.WillRetry != nil {
				retry = *e.WillRetry
			}
			id := e.TurnID
			if id == "" && r.active() {
				id = r.s.Turn
			}
			if err := r.noteRateLimit(id, m.Method, retry, e.Error); err != nil {
				return err
			}
			if err := r.writeProgress(m.Method, fmt.Sprintf("retry=%t %s", retry, r.c.secret.text(e.Error.Message))); err != nil {
				return err
			}
			if !retry && id != "" && r.turnFailure == nil && id != r.s.LastCompleted {
				// Allow the completion frame to provide final items before
				// making the result visible. Some servers send only error.
				r.turnFailure = &pendingTurnFailure{id: id, failure: e.Error, deadline: time.Now().Add(250 * time.Millisecond)}
			}
		}
	case "turn/plan/updated":
		parts := []string{r.c.secret.text(e.Explanation)}
		r.s.PlanStep = ""
		for _, p := range e.Plan {
			step := r.c.secret.text(p.Step)
			if p.Status == "inProgress" && r.s.PlanStep == "" {
				r.s.PlanStep = brief(step)
			}
			parts = append(parts, p.Status+" "+step)
		}
		return r.writeProgress(m.Method, strings.Join(parts, " | "))
	case "turn/diff/updated":
		var files []string
		var oldFile string
		added, removed, gitFiles := 0, 0, 0
		for _, l := range strings.Split(e.Diff, "\n") {
			switch {
			case strings.HasPrefix(l, "diff --git "):
				gitFiles++
			case strings.HasPrefix(l, "--- "):
				oldFile = strings.TrimPrefix(l[4:], "a/")
			case strings.HasPrefix(l, "+++ "):
				path := strings.TrimPrefix(l[4:], "b/")
				if path == "/dev/null" {
					path = oldFile
				}
				files = append(files, path)
			case strings.HasPrefix(l, "+"):
				added++
			case strings.HasPrefix(l, "-"):
				removed++
			}
		}
		r.s.ChangedFiles = gitFiles
		if gitFiles == 0 {
			r.s.ChangedFiles = len(files)
		}
		return r.note(m.Method, fmt.Sprintf("+%d -%d %s", added, removed, strings.Join(files, ", ")))
	case "item/started", "item/completed":
		i := e.Item
		switch i.Type {
		case "commandExecution":
			command := r.c.secret.text(i.Command)
			if r.cmdOpen == nil {
				r.cmdOpen = map[string]time.Time{}
			}
			if m.Method == "item/started" {
				r.cmdOpen[i.ID] = time.Now()
			} else {
				delete(r.cmdOpen, i.ID)
				if i.ExitCode != nil && *i.ExitCode != 0 {
					r.s.FailedCommand = &failedCommand{brief(command), int64(*i.ExitCode)}
				}
			}
			info := i.Status + " " + command
			if i.ExitCode != nil {
				info = fmt.Sprintf("exit=%d %s", *i.ExitCode, info)
			}
			return r.writeProgress(m.Method+" commandExecution", info)
		case "fileChange":
			for _, f := range i.Changes {
				if err := r.writeProgress(m.Method+" fileChange", i.Status+" "+f.Kind.Type+" "+r.c.secret.text(f.Path)); err != nil {
					return err
				}
			}
		case "agentMessage":
			if m.Method == "item/completed" {
				r.lastText = i.Text
				return r.note(m.Method+" agentMessage", i.Text)
			}
		case "plan":
			return r.note(m.Method+" plan", i.Text)
		}
	case "serverRequest/resolved":
		return r.removeApproval(e.RequestID)
	}
	return nil
}

func (r *runner) threadParams() fields {
	p := fields{"cwd": r.c.Dir, "approvalPolicy": appServerApprovalPolicy, "sandbox": r.c.Sandbox}
	if r.c.Model != "" {
		p["model"] = r.c.Model
	}
	cfg := fields{}
	if r.c.Effort != "" {
		cfg["model_reasoning_effort"] = r.c.Effort
	}
	if r.c.WebSearch {
		cfg["web_search"] = "live"
	}
	if len(cfg) > 0 {
		p["config"] = cfg
	}
	return p
}

func (r *runner) connectThread() error {
	p := r.threadParams()
	method := "thread/start"
	previous := r.s.Thread
	if previous == "" && r.c.ForkThread != "" {
		method = "thread/fork"
		p["threadId"] = r.c.ForkThread
		if r.c.Through != "" {
			p["lastTurnId"] = r.c.Through
		}
	}
	if previous != "" {
		method = "thread/resume"
		p["threadId"] = previous
	}
	var response struct {
		Thread struct {
			ID, HistoryMode string
			Status          struct{ Type string }
			Turns           []turn
		}
	}
	var err error
	if method == "thread/fork" {
		err = r.fork(p, &response)
	} else {
		if method == "thread/resume" {
			delete(p, "model")
			delete(p, "config")
			if r.c.WebSearch {
				p["config"] = fields{"web_search": "live"}
			}
		}
		err = r.call(method, p, &response)
	}
	if err != nil {
		return err
	}
	th := response.Thread
	if th.ID == "" || (previous != "" && th.ID != previous) {
		return fmt.Errorf("%s returned inconsistent thread id", method)
	}
	r.s.Thread = th.ID
	if err := r.save(); err != nil {
		return err
	}
	if err := r.writeProgress(method, th.ID); err != nil {
		return err
	}
	if (previous != "" || method == "thread/fork") && th.HistoryMode == "paginated" {
		var page struct{ Data []turn }
		if err := r.call("thread/turns/list", fields{"threadId": th.ID, "sortDirection": "desc", "limit": 1, "itemsView": "full"}, &page); err != nil {
			return err
		}
		th.Turns = page.Data
	}
	var latest turn
	if len(th.Turns) > 0 {
		latest = th.Turns[len(th.Turns)-1]
		r.lastText = ""
		for _, i := range latest.Items {
			if i.Type == "agentMessage" {
				r.lastText = i.Text
			}
		}
		if latest.Error != nil || latest.ID == r.s.LastCompleted && r.s.CompletedStatus == "failed" {
			latest.Status = "failed"
		}
		if latest.Status == "inProgress" && th.Status.Type == "active" {
			if err := r.acquireSlot(); err != nil {
				return err
			}
			if err := r.begin(latest.ID); err != nil {
				return err
			}
			r.turnSince = time.Now()
		} else {
			if latest.Status == "inProgress" {
				latest.Status = "interrupted"
			}
			if latest.ID == r.s.LastCompleted && latest.Status == r.s.CompletedStatus {
				// This completion and last.md were already committed before restart.
				if r.pending == "" {
					r.releaseSlot()
				}
				r.s.Turn = latest.ID
				r.s.TurnStatus = latest.Status
				r.deriveState()
				r.idleSince = time.Now()
				if err := r.save(); err != nil {
					return err
				}
			} else if err := r.finish(latest); err != nil {
				return err
			}
		}
	}
	if err := r.recoverInbox(latest); err != nil {
		return err
	}
	if latest.ID == "" {
		if r.c.ForkThread != "" || r.s.Finished > 0 && r.s.Turn == "" {
			r.releaseSlot()
			r.s.State = "waiting"
			r.idleSince = time.Now()
			return r.save()
		}
		if previous != "" && r.s.Turn != "" {
			return fmt.Errorf("saved turn missing from resumed thread; refusing to replay initial prompt")
		}
		return r.input(r.c.Prompt, "initial", false)
	}
	return nil
}

func (r *runner) input(text, id string, steer bool) error {
	var input any = []fields{{"type": "text", "text": text}}
	if id == "initial" && len(r.c.Input) > 0 {
		input = r.c.Input
	}
	return r.sendInput(input, id, steer)
}
func (r *runner) sendInput(input any, id string, steer bool) error {
	if r.claude != nil {
		return r.startClaude(input, id)
	}
	p := fields{"threadId": r.s.Thread, "clientUserMessageId": id, "input": input}
	if steer {
		p["expectedTurnId"] = r.s.Turn
		if err := r.call("turn/steer", p, nil); err != nil {
			return err
		}
		return r.writeProgress("turn/steer", id+" → "+r.s.Turn)
	}
	if err := r.acquireSlot(); err != nil {
		return err
	}
	sb := fields{"type": "readOnly", "networkAccess": r.c.Network}
	switch r.c.Sandbox {
	case "workspace-write":
		sb["type"] = "workspaceWrite"
		sb["writableRoots"] = append([]string{r.c.Dir}, r.c.Roots...)
	case dangerFullAccess:
		sb = fields{"type": "dangerFullAccess"}
	}
	p["sandboxPolicy"] = sb

	p["approvalPolicy"] = appServerApprovalPolicy
	var result struct{ Turn turn }
	if err := r.call("turn/start", p, &result); err != nil {
		var rejection *rpcError
		if errors.As(err, &rejection) {
			r.releaseSlot()
		}
		return err
	}
	if err := r.writeProgress("turn/start", id+" → "+result.Turn.ID); err != nil {
		return err
	}
	if r.s.LastCompleted == result.Turn.ID {
		return nil
	} // a fast completion may precede the reply
	if result.Turn.Error != nil || result.Turn.Status == "failed" {
		return r.finish(result.Turn)
	}
	return r.begin(result.Turn.ID)
}

func (r *runner) appendEvent(f *os.File, record map[string]json.RawMessage) error {
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	record["seq"] = json.RawMessage(strconv.FormatInt(fi.Size()+1, 10))
	if r.c.privileged() {
		record["privileged"] = json.RawMessage("true")
	}
	return json.NewEncoder(f).Encode(record)
}

// Also works before admission, when the runner has not opened its logs yet.
func (r *runner) localEvent(method, detail string) error {
	events := r.events
	if events == nil {
		var err error
		events, err = os.OpenFile(filepath.Join(r.dir, "events.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		defer closeBestEffort(events)
	}
	name, _ := json.Marshal(method)
	params, _ := json.Marshal(fields{"detail": detail})
	if err := r.appendEvent(events, map[string]json.RawMessage{"method": name, "params": params}); err != nil {
		return err
	}
	if r.log != nil {
		return r.writeProgress(method, detail)
	}
	f, err := os.OpenFile(filepath.Join(r.dir, "progress.log"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer closeBestEffort(f)
	temp := runner{log: f}
	return temp.writeProgress(method, detail)
}
