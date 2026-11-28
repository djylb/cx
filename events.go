package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/djylb/timeshim"
)

type item struct {
	Type, ID, Text, ClientID, Command, Status string
	ExitCode                                  *int
	Path, Cwd                                 string
	CommandActions                            []commandReadAction
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
	Path, Cwd                           string
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
	// ErrorInfo carries the Codex wire field codexErrorInfo; other adapters set RateLimited.
	ErrorInfo   json.RawMessage `json:"codexErrorInfo,omitempty"`
	RateLimited bool            `json:"rateLimited,omitempty"`
}

const failedReportHeading = "STATUS: FAILED"

const turnReportKind = "turn_report"

type turnReportRef struct {
	Metadata string `json:"metadata,omitempty"`
	Turn     string `json:"turn"`
	Path     string `json:"path"`
	SHA256   string `json:"sha256"`
	Size     int64  `json:"size"`
}

func turnReportPath(id string) (string, error) {
	if _, err := jobPath("", id); err != nil {
		return "", fmt.Errorf("invalid turn report id %q", id)
	}
	return "turns/" + id + ".md", nil
}

func turnReportRecords(dir string) (map[string]stateRecord, error) {
	tail, err := openStateTail(dir, stateCursor{})
	if err != nil {
		return nil, err
	}
	defer closeBestEffort(tail)
	records, err := tail.Next()
	if err != nil {
		return nil, err
	}
	reports := make(map[string]stateRecord)
	for _, record := range records {
		if record.Kind != turnReportKind {
			continue
		}
		var ref turnReportRef
		if err := json.Unmarshal(record.Value, &ref); err != nil {
			return nil, err
		}
		if ref.Turn != "" {
			reports[ref.Turn] = record
		}
	}
	return reports, nil
}

func turnReportCheckpoints(dir string) ([]stateRecord, error) {
	reports, err := turnReportRecords(dir)
	if err != nil {
		return nil, err
	}
	records := make([]stateRecord, 0, len(reports))
	for _, record := range reports {
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Seq < records[j].Seq })
	return records, nil
}

func findTurnReport(dir, id string) (turnReportRef, error) {
	var ref turnReportRef
	if id == "" {
		return ref, nil
	}
	record, _, err := latestStateKind(dir, turnReportKind)
	if err != nil || record.Seq == 0 {
		return ref, err
	}
	if err = json.Unmarshal(record.Value, &ref); err != nil || ref.Turn == id {
		return ref, err
	}
	reports, err := turnReportRecords(dir)
	if err != nil {
		return turnReportRef{}, err
	}
	ref = turnReportRef{}
	if record, ok := reports[id]; ok {
		err = json.Unmarshal(record.Value, &ref)
	}
	return ref, err
}

func openJobContent(ctx context.Context, path string) (*os.File, error) {
	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	for {
		f, err := openJobFile(path, jobFileRead)
		if err == nil {
			return f, nil
		}
		if pause(ctx, 10*time.Millisecond) != nil {
			return nil, err
		}
	}
}

func readTurnReport(ctx context.Context, dir, id string) ([]byte, error) {
	ref, err := findTurnReport(dir, id)
	if err != nil || ref.Turn == "" {
		return nil, err
	}
	path, err := turnReportPath(id)
	base := strings.TrimSuffix(path, ".md") + "-"
	nonce, retryPath := strings.CutPrefix(strings.TrimSuffix(ref.Path, ".md"), base)
	retryPath = retryPath && strings.HasSuffix(ref.Path, ".md") && snapHex(nonce, 32)
	if err != nil || (ref.Path != path && !retryPath) || ref.Size < 0 || !snapHex(ref.SHA256, 64) {
		return nil, fmt.Errorf("invalid report reference for turn %q", id)
	}
	f, err := openJobContent(ctx, filepath.Join(dir, filepath.FromSlash(ref.Path)))
	if err != nil {
		return nil, err
	}
	defer closeBestEffort(f)
	b, err := io.ReadAll(io.LimitReader(f, ref.Size))
	if err != nil {
		return nil, err
	}
	var extra [1]byte
	if n, err := f.Read(extra[:]); n != 0 || err != io.EOF {
		return nil, fmt.Errorf("report for turn %q has content beyond its reference: %v", id, err)
	}
	if int64(len(b)) != ref.Size || snapSum(b) != ref.SHA256 {
		return nil, fmt.Errorf("report for turn %q does not match its state record", id)
	}
	return b, nil
}

func (r *runner) publishTurnReport(id string, report []byte) error {
	return r.publishTurnReportMetadata(id, report, "")
}

func (r *runner) publishTurnReportMetadata(id string, report []byte, metadata string) error {
	path, err := turnReportPath(id)
	if err != nil {
		return err
	}
	full := filepath.Join(r.dir, filepath.FromSlash(path))
	if mkdirErr := os.MkdirAll(filepath.Dir(full), 0700); mkdirErr != nil {
		return mkdirErr
	}
	ref := turnReportRef{Turn: id, Path: path, Size: int64(len(report)), SHA256: snapSum(report), Metadata: metadata}
	previous, err := findTurnReport(r.dir, id)
	if err != nil || previous.Turn != "" {
		return err
	}
	f, err := openJobFile(full, jobFileNew)
	if errors.Is(err, os.ErrExist) {
		// An interrupted write left unpublished bytes. Use a fresh name.
		var nonce [16]byte
		_, _ = rand.Read(nonce[:])
		ref.Path = strings.TrimSuffix(path, ".md") + fmt.Sprintf("-%x.md", nonce)
		f, err = openJobFile(filepath.Join(r.dir, filepath.FromSlash(ref.Path)), jobFileNew)
	}
	if err != nil {
		return err
	}
	_, err = f.Write(report)
	if err == nil {
		err = syncJobFile(f)
	}
	if writeErr := errors.Join(err, f.Close()); writeErr != nil {
		return writeErr
	}
	if err := syncJobTree(filepath.Dir(full), r.dir); err != nil {
		return err
	}
	return r.stateLog.Append(turnReportKind, ref)
}

type turnCompletion struct {
	ResetAt *int64 `json:"reset_at,omitempty"`
	Outcome string `json:"outcome"`
	observedResult
	RateLimited bool `json:"rate_limited,omitempty"`
}

func completedTurn(s status) turnCompletion {
	outcome := s.CompletedStatus
	if s.final && s.Turn == s.LastCompleted && s.TurnStatus != "" && s.TurnStatus != "inProgress" {
		// Closing a queued continuation can update the job's outcome without
		// changing the result of the prior turn.
		outcome = s.TurnStatus
	}
	return turnCompletion{
		Outcome:        outcome,
		observedResult: observedResult{RunID: s.RunID, Number: s.Finished, TurnID: s.LastCompleted},
		RateLimited:    s.CompletedRateLimited,
		ResetAt:        s.CompletedResetAt,
	}
}

func completionCheckpoints(dir string) ([]stateRecord, error) {
	tail, err := openStateTail(dir, stateCursor{})
	if err != nil {
		return nil, err
	}
	defer closeBestEffort(tail)
	records, err := tail.Next()
	if err != nil {
		return nil, err
	}
	var order []int64
	turns := make(map[int64]turnCompletion)
	for _, record := range records {
		var turn turnCompletion
		switch record.Kind {
		case "turn_completion":
			err = json.Unmarshal(record.Value, &turn)
		case "status":
			var s status
			err = decodeJobStatus(record, &s)
			turn = completedTurn(s)
		default:
			continue
		}
		if err != nil {
			return nil, err
		}
		if turn.Number == 0 || turn.TurnID == "" {
			continue
		}
		if prior, seen := turns[turn.Number]; !seen {
			order = append(order, turn.Number)
		} else if prior.RunID != turn.RunID || prior.TurnID != turn.TurnID {
			continue
		}
		turns[turn.Number] = turn
	}
	checkpoints := make([]stateRecord, 0, len(order))
	for _, number := range order {
		frame, err := newRecord("turn_completion", turns[number])
		if err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, frame)
	}
	return checkpoints, nil
}

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
		if len(e.ErrorInfo) != 0 && string(e.ErrorInfo) != "null" {
			report += "errorInfo=" + r.c.secret.text(string(e.ErrorInfo)) + "\n"
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
	if f := r.turnFailure; f != nil && !timeshim.Or(r.clock).Now().Before(f.deadline) {
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
// status columns must agree with the state snapshot and the unmodified protocol.
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
		r.turnSince = timeshim.Or(r.clock).Now()
	}
	r.s.InputKind = ""
	r.s.Turn = id
	r.s.TurnStatus = "inProgress"
	r.deriveState()
	return r.save()
}
func (r *runner) finish(t turn) error { return r.finishTurn(t, false) }

func (r *runner) finishTurn(t turn, live bool) error {
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
	// The first report is immutable once published. The error notification
	// grace period lets a closely following completion supply its final items.
	alreadyCompleted := t.ID == r.s.LastCompleted
	if alreadyCompleted && (r.s.CompletedStatus == "failed" || t.Status == r.s.CompletedStatus) {
		return nil
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
		paths, source := codexReadPaths(i)
		r.recordReadPaths(t.ID, i.Cwd, paths, source)
		if i.Type == "agentMessage" {
			r.lastText = i.Text
		}
	}
	report := r.c.secret.reportText(r.lastText)
	if t.Status == "failed" {
		r.s.Approvals = nil
		if report != "" {
			report = "\n" + report
		}
		report = r.describeFailure(t.Error) + report
	}
	var usage *usageSummary
	if live && r.usageTurn == t.ID && r.usageGeneration == r.s.Generation {
		usage = readUsage(r.s.Usage)
		if usage != nil {
			usage.Total = nil
		}
	}
	if live {
		r.s.TurnContext = nil
		if usage != nil {
			r.s.TurnContext = usage.ContextPercent
		}
	}
	warning := r.usageWarning(usage)
	if err := r.publishAgentTurn(t, []byte(report), usage, warning, !alreadyCompleted && live); err != nil {
		return err
	}
	check := !alreadyCompleted && live && (t.Status == "completed" || t.Status == "failed") && hasAcceptance(r.c.Work)
	r.releaseSlot()
	r.s.Turn = t.ID
	r.s.TurnStatus = t.Status
	if t.ID != r.s.LastCompleted {
		r.s.Finished++
		r.s.LastCompleted = t.ID
	}
	r.s.CompletedStatus = t.Status
	r.s.CompletedRateLimited = t.Error != nil && t.Error.rateLimited()
	now := timeshim.Or(r.clock).Now()
	r.s.CompletedResetAt = nil
	if r.s.CompletedRateLimited {
		_, r.s.CompletedResetAt = r.s.windowResets(now.Unix())
	}
	r.s.InputKind = ""
	line, _, _ := strings.Cut(r.lastText, "\n")
	if (r.c.Interactive || strings.HasPrefix(t.ID, "refresh-")) && t.Status == "completed" && reportStatus(line) == "QUESTION" {
		r.s.InputKind = "next_turn"
	}
	r.deriveState()
	r.idleSince = now
	info := t.ID + " " + t.Status
	if t.Error != nil {
		info += " " + r.c.secret.text(t.Error.Message)
	}
	if !alreadyCompleted {
		if err := r.writeProgress("turn/completed", info); err != nil {
			return err
		}
	}
	if err := r.save(); err != nil {
		return err
	}
	if !alreadyCompleted {
		r.notifyContextWarning(warning)
	}
	if check {
		folder, err := sealTurn(r.ctx, r.dir, r.s, r.s.Finished)
		if err != nil {
			// The next successful seal of this turn delivers the acceptance.
			r.acceptRetry = r.s.Finished
			return r.writeProgress("result/seal-failed", r.c.secret.text(err.Error()))
		}
		return r.acceptSealedTurn(folder)
	}
	if t.Status == "completed" {
		if err := r.sealSnapshotTurn(); err != nil {
			return r.writeProgress("result/seal-failed", r.c.secret.text(err.Error()))
		}
	}
	return nil
}
func (r *runner) notify(m message) error {
	if m.Method == "item/agentMessage/delta" || m.Method == "item/commandExecution/outputDelta" {
		r.s.StallCount = 0
	}
	// Deltas remain in events.jsonl. Completed items carry full text.
	switch m.Method {
	case "turn/started", "turn/completed", "turn/plan/updated", "turn/diff/updated", "item/started", "item/completed", "file/read", "item/fileRead", "thread/tokenUsage/updated", "account/rateLimits/updated", "error", "serverRequest/resolved":
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
		return r.finishTurn(e.Turn, r.startingTurn || r.liveTurn == e.Turn.ID)
	case "thread/tokenUsage/updated":
		if e.TurnID != "" {
			sameTurn := r.usageTurn == e.TurnID && r.usageGeneration == r.s.Generation
			next, sample := retainEngineUsage(e.TokenUsage, r.s.Usage, sameTurn)
			r.s.Usage = next
			if sample {
				r.usageTurn, r.usageGeneration = e.TurnID, r.s.Generation
			}
		}
	case "account/rateLimits/updated":
		r.s.update(e.RateLimits)
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
				r.turnFailure = &pendingTurnFailure{id: id, failure: e.Error, deadline: timeshim.Or(r.clock).Now().Add(250 * time.Millisecond)}
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
		for l := range strings.SplitSeq(e.Diff, "\n") {
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
	case "file/read", "item/fileRead":
		r.recordReadPaths(e.TurnID, e.Cwd, []string{e.Path}, "structured")
	case "item/started", "item/completed":
		i := e.Item
		turnID := e.TurnID
		if turnID == "" && r.active() {
			turnID = r.s.Turn
		}
		if m.Method == "item/completed" {
			paths, source := codexReadPaths(i)
			r.recordReadPaths(turnID, i.Cwd, paths, source)
		}
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

// Resolve paths from the main repository root, including sibling members.
func ownedMember(cwd string, members []string, path string) (int, string, error) {
	main := containingMember(members, cwd)
	if main < 0 || path == "" || filepath.IsAbs(path) || filepath.VolumeName(path) != "" ||
		strings.HasPrefix(path, string(filepath.Separator)) || strings.ContainsAny(path, "\\\x00\r\n") {
		return -1, "", fmt.Errorf("invalid snapshot --own path %q", path)
	}
	target := filepath.Join(members[main], path)
	index := containingMember(members, target)
	if index < 0 {
		return -1, "", fmt.Errorf("--own %q is outside snapshot repositories", path)
	}
	rel, relErr := filepath.Rel(members[index], target)
	return index, rel, relErr
}

func containingMember(members []string, path string) int {
	for i, member := range members {
		if snapWithin(member, path) {
			return i
		}
	}
	return -1
}

func (c *config) writableMemberRoots(root string) ([]string, error) {
	members, rootsErr := snapshotMemberRoots(root, c.Dir)
	if rootsErr != nil {
		return nil, rootsErr
	}
	if len(c.Own) == 0 {
		return members, nil
	}
	cwd, cwdErr := filepath.EvalSymlinks(c.Dir)
	if cwdErr != nil {
		return nil, cwdErr
	}
	roots := make([]string, 0, len(c.Own))
	for _, path := range c.Own {
		index, rel, pathErr := ownedMember(cwd, members, path)
		if pathErr != nil {
			return nil, pathErr
		}
		target := filepath.Join(members[index], rel)
		physical, resolveErr := filepath.EvalSymlinks(target)
		if resolveErr != nil || physical != target {
			return nil, fmt.Errorf("--own %q must name an existing snapshot file or directory without symlinks", path)
		}
		roots = append(roots, target)
	}
	return roots, nil
}

const ownedPermissions = "cx-owned"

func (r *runner) codexThreadParams() (fields, error) {
	p := r.threadParams()
	if len(r.c.Own) == 0 {
		return p, nil
	}
	roots, rootsErr := r.c.writableMemberRoots(r.root)
	if rootsErr != nil {
		return nil, rootsErr
	}
	// The legacy workspace-write policy implicitly grants writes to cwd.
	delete(p, "sandbox")
	p["permissions"] = ownedPermissions
	cfg, ok := p["config"].(fields)
	if !ok {
		cfg = fields{}
		p["config"] = cfg
	}
	fs := fields{":root": "read"}
	for _, root := range append(roots, r.c.Roots...) {
		fs[root] = "write"
	}
	cfg["permissions."+ownedPermissions] = fields{
		"filesystem": fs,
		"network":    fields{"enabled": r.c.Network},
	}
	return p, nil
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
	p, paramsErr := r.codexThreadParams()
	if paramsErr != nil {
		return paramsErr
	}
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
	}
	// The engine's history omits the node-generated refresh question.
	if strings.HasPrefix(r.s.LastCompleted, "refresh-") && (latest.Status != "inProgress" || th.Status.Type != "active") {
		r.releaseSlot()
		r.deriveState()
		r.idleSince = timeshim.Or(r.clock).Now()
		if err := r.save(); err != nil {
			return err
		}
		return r.recoverInbox(latest)
	}
	if latest.ID != "" {
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
			if previous != "" && r.s.Turn == latest.ID {
				r.liveTurn = latest.ID
			}
			if err := r.acquireSlot(); err != nil {
				return err
			}
			if err := r.begin(latest.ID); err != nil {
				return err
			}
			r.turnSince = timeshim.Or(r.clock).Now()
		} else {
			if latest.Status == "inProgress" {
				latest.Status = "interrupted"
			}
			if latest.ID == r.s.LastCompleted && latest.Status == r.s.CompletedStatus {
				// This completion and its report were already published.
				if r.pending == "" {
					r.releaseSlot()
				}
				r.s.Turn = latest.ID
				r.s.TurnStatus = latest.Status
				r.deriveState()
				r.idleSince = timeshim.Or(r.clock).Now()
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
		if previous != "" && r.hasUncertainRevert() {
			r.clearCurrentTurn()
		}
		if r.c.ForkThread != "" || r.s.Finished > 0 && r.s.Turn == "" {
			r.releaseSlot()
			r.s.State = "waiting"
			r.idleSince = timeshim.Or(r.clock).Now()
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
	if id == "initial" {
		parts := slices.DeleteFunc([]string{r.c.Preamble, r.moduleNote, text}, func(s string) bool { return s == "" })
		text = strings.Join(parts, "\n\n")
		if r.c.Input != nil {
			items := r.c.Input
			if r.c.Prompt != "" {
				items = items[1:] // Replace the original text item built by options.inputs.
			}
			if text != "" {
				initial, err := json.Marshal(fields{"type": "text", "text": text})
				if err != nil {
					return err
				}
				items = append([]json.RawMessage{initial}, items...)
			}
			if len(items) == 0 {
				return errors.New("empty input")
			}
			return r.sendInput(items, id, steer)
		}
	}
	return r.sendInput([]fields{{"type": "text", "text": text}}, id, steer)
}
func (r *runner) sendInput(input any, id string, steer bool) (err error) {
	if !steer && r.s.Finished > 0 && r.s.LastCompleted != "" && canSealResult(r.s) {
		folder, sealErr := sealTurn(r.ctx, r.dir, r.s, r.s.Finished)
		if sealErr != nil {
			return fmt.Errorf("seal previous turn before starting the next: %w", sealErr)
		}
		if r.acceptRetry == r.s.Finished {
			r.acceptRetry = 0
			if err = r.acceptSealedTurn(folder); err != nil {
				return err
			}
		}
	}
	if !steer {
		if err = r.acquireSlot(); err != nil {
			return err
		}
		if question, refreshErr := r.refreshBeforeTurn(); question || refreshErr != nil {
			return refreshErr
		}
	}
	if !steer && r.closeRequest != 0 {
		return errQueuedClose
	}
	if !steer && r.refreshModuleNote != "" {
		part := fields{"type": "text", "text": r.refreshModuleNote}
		switch items := input.(type) {
		case []fields:
			input = append([]fields{part}, items...)
		case []json.RawMessage:
			raw, marshalErr := json.Marshal(part)
			if marshalErr != nil {
				return marshalErr
			}
			input = append([]json.RawMessage{raw}, items...)
		}
		// Keep the note until the engine accepts this input, so a failed start
		// or a restart delivers it again.
		defer func() {
			if err == nil {
				r.refreshModuleNote = ""
			}
		}()
	}
	if r.claude != nil {
		return r.startClaude(input, id)
	}
	p := fields{"threadId": r.s.Thread, "clientUserMessageId": id, "input": input}
	if steer {
		p["expectedTurnId"] = r.s.Turn
		if err = r.call("turn/steer", p, nil); err != nil {
			return err
		}
		return r.writeProgress("turn/steer", id+" → "+r.s.Turn)
	}
	if len(r.c.Own) == 0 {
		sb := fields{"type": "readOnly", "networkAccess": r.c.Network}
		switch r.c.Sandbox {
		case "workspace-write":
			sb["type"] = "workspaceWrite"
			roots, rootsErr := r.c.writableMemberRoots(r.root)
			if rootsErr != nil {
				return rootsErr
			}
			if len(roots) == 0 {
				roots = []string{r.c.Dir}
			}
			sb["writableRoots"] = append(roots, r.c.Roots...)
		case dangerFullAccess:
			sb = fields{"type": "dangerFullAccess"}
		}
		p["sandboxPolicy"] = sb
	}

	p["approvalPolicy"] = appServerApprovalPolicy
	var result struct{ Turn turn }
	r.startingTurn = true
	err = r.call("turn/start", p, &result)
	r.startingTurn = false
	if err != nil {
		if _, ok := errors.AsType[*rpcError](err); ok {
			r.releaseSlot()
		}
		return err
	}
	r.liveTurn = result.Turn.ID
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
	if string(record["method"]) == `"item/completed"` {
		record["time"], _ = time.Now().UTC().MarshalJSON()
		record["turn_number"] = json.RawMessage(strconv.FormatInt(r.s.Finished+1, 10))
	}
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
		events, err = openJobFile(filepath.Join(r.dir, "events.jsonl"), jobFileAppendCreate)
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
	f, err := openJobFile(filepath.Join(r.dir, "progress.log"), jobFileAppend)
	if err != nil {
		return err
	}
	defer closeBestEffort(f)
	temp := runner{log: f}
	return temp.writeProgress(method, detail)
}

// appendProgress records a line without an open runner log, best effort.
func appendProgress(dir, kind, detail string) {
	f, err := openJobFile(filepath.Join(dir, "progress.log"), jobFileAppend)
	if err != nil {
		return
	}
	defer closeBestEffort(f)
	if fi, err := f.Stat(); err == nil {
		_, _ = fmt.Fprintf(f, "%d\t%s\t%s\t%s\n", fi.Size()+1, time.Now().UTC().Format(time.RFC3339Nano), kind, brief(detail))
	}
}
