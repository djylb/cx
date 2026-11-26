package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

// The subprocess fixture exercises real pipes, files and locks. It needs no model account.
func TestMain(m *testing.M) {
	if os.Getenv("GORACE") == "" {
		_ = os.Setenv("GORACE", "atexit_sleep_ms=0")
	}
	// Keep the controller configuration (context layers, projects) of the machine
	// running the tests out of every test; tests that need it set their own.
	if os.Getenv("CX_TEST_CONFIG") == "" {
		dir, err := os.MkdirTemp("", "cx-test-config-")
		if err != nil {
			panic(err)
		}
		_ = os.Setenv("CX_TEST_CONFIG", dir)
		_ = os.Setenv("XDG_CONFIG_HOME", dir)
	}
	// Tests that exercise the disk watermark set min_free_bytes explicitly.
	defaultMinFreeBytes, defaultMinFreeDivisor = 0, 0
	trackFixtureWorkerStarts()
	if base := filepath.Base(os.Args[0]); base == "cx" || base == "cxrun" {
		main()
		return
	}
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "fixture-command":
			os.Exit(commandFixture(os.Args[2:]))
		case "_command":
			commandFixtureMain()
			return
		case "node":
			main()
			return
		case "fixture-cli":
			os.Args = os.Args[1:]
			os.Args[0] = "cxrun"
			main()
			return
		case "app-server":
			if err := fixtureServer(); err != nil {
				diagnosticf("%v\n", err)
				os.Exit(1)
			}
			return
		case "check-files":
			for _, p := range []string{"one", "two", "three"} {
				b, err := os.ReadFile(p)
				if err != nil || string(b) != p {
					os.Exit(1)
				}
			}
			return
		}
	}
	os.Exit(m.Run())
}

const fixtureWorkerLock = "CX_TEST_WORKER_LOCK"

func trackFixtureWorkerStarts() {
	start := startDetachedWorker
	startDetachedWorker = func(cmd *exec.Cmd) error {
		path := os.Getenv(fixtureWorkerLock)
		if path == "" || runtime.GOOS == "windows" {
			return start(cmd)
		}
		lease, err := lockFile(path, os.O_CREATE, trySharedLock)
		if err != nil {
			return err
		}
		if lease == nil {
			return errors.New("fixture is already joining its workers")
		}
		defer closeBestEffort(lease)
		// Hold the lease from spawn, including when the CLI can return
		// before the worker's first instruction (such as fetch-turn).
		cmd.ExtraFiles = append(cmd.ExtraFiles, lease)
		return start(cmd)
	}
}

func joinFixtureWorkers(t *testing.T, dir string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		return // ExtraFiles and inherited flock ownership are Unix-only.
	}
	path := filepath.Join(dir, "workers.lock")
	t.Setenv(fixtureWorkerLock, path)
	t.Cleanup(func() {
		// The runner can still maintain caches after publishing terminal
		// state and releasing owner.lock. Join before TempDir removes state.
		ctx, cancel := context.WithTimeout(context.Background(), fixtureLimit)
		defer cancel()
		barrier, err := lockUntil(ctx, path)
		if err != nil {
			t.Errorf("waiting for detached fixture workers: %v", err)
			return
		}
		closeBestEffort(barrier)
	})
}

func fixtureServer() error {
	in := json.NewDecoder(os.Stdin)
	out := json.NewEncoder(os.Stdout)
	send := func(v fields) {
		if err := out.Encode(v); err != nil {
			panic(err)
		}
	}
	notify := func(m string, v fields) { v["threadId"] = "thread"; send(fields{"method": m, "params": v}) }
	turnID := "first"
	itemEvent := func(i fields) { notify("item/completed", fields{"turnId": turnID, "item": i}) }
	writeFile := func(name string) error {
		if err := os.WriteFile(name, []byte(name), 0600); err != nil {
			return err
		}
		itemEvent(fields{"type": "fileChange", "status": "completed", "changes": []fields{{"kind": fields{"type": "add"}, "path": name}}})
		return nil
	}
	started := false
	var steerID json.RawMessage
	wantNudge := "No progress detected. Report status, abandon any stuck command, and continue the task."
	wantContinue := "Continue the unfinished task."
	if custom := os.Getenv("FIXNUDGE"); custom != "" {
		wantNudge, wantContinue = custom, custom
	}
	wantPrompt := os.Getenv("FIXPROMPT")
	if wantPrompt == "" {
		wantPrompt = "task"
	}
	wantSandbox := os.Getenv("FIXSANDBOX")
	if wantSandbox == "" {
		wantSandbox = "workspace-write"
	}
	for {
		var m message
		if err := in.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		var p struct {
			ThreadID, ExpectedTurnID, ApprovalPolicy, Sandbox string
			Input                                             []struct{ Text string }
			SandboxPolicy                                     struct {
				Type          string
				WritableRoots []string
				NetworkAccess bool
			}
		}
		if len(m.Params) > 0 {
			if err := json.Unmarshal(m.Params, &p); err != nil {
				return err
			}
		}
		if m.Method == "thread/start" || m.Method == "thread/resume" {
			if p.ApprovalPolicy != "never" || p.Sandbox != wantSandbox {
				return fmt.Errorf("%s policy mismatch: %s", m.Method, m.Params)
			}
		}
		result := fields{}
		switch m.Method {
		case "initialize":
		case "initialized":
			continue
		case "thread/resume":
			if p.ThreadID != "thread" {
				return errors.New("wrong resumed thread")
			}
			outcome := "interrupted"
			if os.Getenv("FIXMODE") == "wait-enospc" {
				outcome = "completed"
			}
			turns := []fields{{"id": "first", "status": outcome}}
			if os.Getenv("FIXMODE") == "revert-empty" {
				turns = []fields{}
			}
			result = fields{"thread": fields{"id": "thread", "turns": turns}}
		case "thread/fork":
			var params struct{ ThreadID, LastTurnID string }
			if err := json.Unmarshal(m.Params, &params); err != nil || params.ThreadID != "thread" || params.LastTurnID != "first" {
				return fmt.Errorf("bad fork params %s", m.Params)
			}
			result = fields{"thread": fields{"id": "child"}}
		case "thread/settings/update", "turn/settings/update":
			var params struct{ ThreadID, TurnID, Effort string }
			if err := json.Unmarshal(m.Params, &params); err != nil || params.ThreadID != "thread" || params.Effort != "low" || (m.Method == "turn/settings/update" && params.TurnID != "first") {
				return fmt.Errorf("bad settings params %s", m.Params)
			}
		case "thread/revert":
			var params struct{ BeforeTurnID string }
			if err := json.Unmarshal(m.Params, &params); err != nil || params.BeforeTurnID != "first" {
				return fmt.Errorf("bad revert %s", m.Params)
			}
		case "thread/turns/list":
			turns := []fields{{"id": "prior", "status": "completed", "items": []fields{{"type": "agentMessage", "text": "restored"}}}}
			if os.Getenv("FIXMODE") == "revert-empty" {
				turns = []fields{}
			}
			result = fields{"data": turns}
		case "fixture/noop":
		case "fixture/disconnect":
			return nil
		case "fixture/complete":
			notify("turn/completed", fields{"turn": fields{"id": turnID, "status": "completed"}})
		case "thread/compact/start":
			notify("item/completed", fields{"turnId": turnID, "item": fields{"type": "contextCompaction"}})
		case "fixture/requests":
			for i, method := range fixtureRequests {
				send(fields{"id": i, "method": method, "params": fields{"questions": []fields{{"id": "q"}}}})
			}
			seen := map[int]bool{}
			for range fixtureRequests {
				var reply message
				if err := in.Decode(&reply); err != nil {
					return err
				}
				var id int
				if err := json.Unmarshal(reply.ID, &id); err != nil || id < 0 || id >= len(fixtureRequests) || seen[id] {
					return fmt.Errorf("unexpected reply %s", reply.ID)
				}
				seen[id] = true
				if id >= 7 {
					if reply.Error == nil || reply.Error.Code != -32001 {
						return fmt.Errorf("provider reply %+v", reply)
					}
				} else if !json.Valid(reply.Result) {
					return fmt.Errorf("missing result %+v", reply)
				}
			}
		case "review/start":
			var params struct{ Delivery, ThreadID string }
			if err := json.Unmarshal(m.Params, &params); err != nil || params.ThreadID != "review" || params.Delivery != "inline" {
				return fmt.Errorf("bad review %s", m.Params)
			}
			result = fields{"reviewThreadId": "review", "turn": fields{"id": "reviewTurn"}}
		case "thread/start":
			id := "thread"
			if started {
				id = "review"
			}
			started = true
			result = fields{"thread": fields{"id": id}}
		case "turn/start":
			if p.ThreadID != "thread" || p.ApprovalPolicy != "never" {
				return fmt.Errorf("turn policy mismatch: %s", m.Params)
			}
			if wantSandbox == "danger-full-access" {
				var params struct{ SandboxPolicy map[string]string }
				if err := json.Unmarshal(m.Params, &params); err != nil || len(params.SandboxPolicy) != 1 || params.SandboxPolicy["type"] != "dangerFullAccess" {
					return fmt.Errorf("privileged turn policy mismatch: %s", m.Params)
				}
			} else if p.SandboxPolicy.Type != "workspaceWrite" || !p.SandboxPolicy.NetworkAccess {
				return fmt.Errorf("workspace turn policy mismatch: %s", m.Params)
			}
			if len(p.Input) != 1 {
				return errors.New("expected text input")
			}
			if p.Input[0].Text == wantContinue && os.Getenv("FIXMODE") != "" {
				turnID = "continued"
			} else if p.Input[0].Text == "second round" {
				turnID = "second"
			} else if p.Input[0].Text != wantPrompt {
				return fmt.Errorf("initial prompt: got %q, want %q", p.Input[0].Text, wantPrompt)
			}
			if path := os.Getenv("FIXTURE_PACKAGE_CACHE_RECORD"); path != "" {
				record := packageCacheRecord{Env: packageCacheFixtureEnv(), WritableRoots: p.SandboxPolicy.WritableRoots}
				if err := writeJSON(path, record); err != nil {
					return err
				}
			}
			mode := os.Getenv("FIXMODE")
			if turnID == "second" && (mode == "wait-followup" || mode == "wait-enospc") {
				if mode == "wait-enospc" && started {
					t := fields{"id": "lost", "status": "inProgress"}
					notify("turn/started", fields{"turn": t})
					send(fields{"id": m.ID, "result": fields{"turn": t}})
					return errors.New("ENOSPC: no space left on device")
				}
				ctx, cancel := context.WithTimeout(context.Background(), fixtureLimit)
				err := waitFixtureStart(ctx)
				cancel()
				if err != nil {
					return err
				}
			}
			t := fields{"id": turnID, "status": "inProgress"}
			result = fields{"turn": t}
			notify("turn/started", result)
			switch os.Getenv("FIXMODE") {
			case "sealed-patches":
				content := map[string]string{"a": "round 1\n", "undo": "round 1\n"}
				if turnID == "second" {
					content = map[string]string{"a": "round 2\n", "undo": "base\n", "new": "round 2\n"}
				}
				for name, text := range content {
					if err := os.WriteFile(name, []byte(text), 0600); err != nil {
						return err
					}
				}
				notify("turn/completed", fields{"turn": fields{"id": turnID, "status": "completed", "items": []fields{{"type": "agentMessage", "text": "STATUS: DONE"}}}})
			case "question":
				text := "STATUS: QUESTION\nNext?"
				if turnID == "second" {
					text = "STATUS: DONE"
				}
				notify("turn/completed", fields{"turn": fields{"id": turnID, "status": "completed", "items": []fields{{"type": "agentMessage", "text": text}}}})
			case "failure-pair", "failure-only", "failure-eof", "failure-status", "failure-field", "failure-retry":
				mode := os.Getenv("FIXMODE")
				failure := fields{"message": "This content was flagged for possible cybersecurity risk", "code": 431, "codexErrorInfo": "other"}
				report := fields{"type": "agentMessage", "text": "last agent message"}
				final := fields{"id": turnID, "status": "failed"}
				switch mode {
				case "failure-pair":
					notify("error", fields{"turnId": turnID, "willRetry": false, "error": failure})
					final["items"] = []fields{report}
				case "failure-only", "failure-eof":
					itemEvent(report)
					notify("error", fields{"turnId": turnID, "retry": false, "error": failure})
					if mode == "failure-eof" {
						return nil
					}
				case "failure-status":
					itemEvent(report)
				case "failure-field":
					final["status"] = "completed"
					final["error"] = failure
					final["items"] = []fields{report}
				case "failure-retry":
					itemEvent(report)
					notify("error", fields{"turnId": turnID, "willRetry": true, "error": failure})
					final["status"] = "completed"
				}
				if mode != "failure-only" {
					notify("turn/completed", fields{"turn": final})
				}
			case "crash-loop":
				send(fields{"id": m.ID, "result": result})
				return nil
			case "command-silent":
				notify("item/started", fields{"turnId": turnID, "item": fields{"type": "commandExecution", "id": "cmd"}})
			case "wait-followup", "wait-enospc":
				if turnID == "first" {
					notify("turn/completed", fields{"turn": fields{"id": turnID, "status": "completed"}})
				}
			case "silent", "manual":
				break
			case "scale":
				if turnID != "first" {
					break
				}
				notify("turn/plan/updated", fields{"turnId": turnID, "plan": []fields{
					{"step": "prepare", "status": "completed"},
					{"step": " run  checks ", "status": "inProgress"},
					{"step": "report", "status": "pending"},
				}})
				notify("turn/diff/updated", fields{"turnId": turnID, "diff": "diff --git a/old b/new\nrename from old\nrename to new\n"})
				notify("turn/diff/updated", fields{"turnId": turnID, "diff": "diff --git a/old b/new\nrename from old\nrename to new\ndiff --git a/img b/img\nBinary files a/img and b/img differ\n"})
				itemEvent(fields{"type": "commandExecution", "command": "test  fixture", "exitCode": 17})
				itemEvent(fields{"type": "commandExecution", "command": "ok", "exitCode": 0})
				notify("thread/tokenUsage/updated", fields{"turnId": turnID, "tokenUsage": fields{"total": fields{"totalTokens": 42}}})
				notify("account/rateLimits/updated", fields{"rateLimits": fields{
					"limitId": "codex", "primary": fields{"usedPercent": 64, "resetsAt": int64(4102444800)},
				}})
				notify("account/rateLimits/updated", fields{"rateLimits": fields{
					"primary": nil, "secondary": fields{"usedPercent": 0, "resetsAt": int64(4102448400)},
				}})
				notify("error", fields{"turnId": turnID, "willRetry": true, "error": fields{
					"message": "provider rejected attempt", "codexErrorInfo": fields{"responseTooManyFailedAttempts": fields{"httpStatusCode": 429}},
				}})
				notify("turn/completed", fields{"turn": fields{"id": turnID, "status": "failed", "error": fields{
					"message": "quota unavailable", "codexErrorInfo": "usageLimitExceeded",
				}}})
			case "exit":
				if turnID == "first" {
					send(fields{"id": m.ID, "result": result})
					return nil
				}
				fallthrough
			case "credential":
				itemEvent(fields{"type": "agentMessage", "text": os.Getenv("AUTH_VALUE")})
				notify("turn/completed", fields{"turn": fields{"id": turnID, "status": "completed"}})
			case "complete", "revert-empty":
				notify("turn/completed", fields{"turn": fields{"id": turnID, "status": "completed"}})
			default:
				if turnID == "first" {
					notify("turn/plan/updated", fields{"turnId": turnID, "plan": []fields{{"step": "write files", "status": "inProgress"}}})
					for _, name := range []string{"one", "two"} {
						if err := writeFile(name); err != nil {
							return err
						}
					}
				}
			}
		case "turn/steer":
			if os.Getenv("FIXMODE") == "manual" {
				switch p.Input[0].Text {
				case "hello":
					// Agent inbox tests acknowledge a steer without ending the turn.
				case "completed", "failed":
					notify("turn/completed", fields{"turn": fields{"id": turnID, "status": p.Input[0].Text}})
				default:
					return errors.New("unexpected test steer")
				}
				break
			}
			if (os.Getenv("FIXMODE") == "silent" || os.Getenv("FIXMODE") == "command-silent") && p.Input[0].Text == wantNudge {
				break
			}
			if p.ExpectedTurnID != "first" || len(p.Input) != 1 || p.Input[0].Text != "add three" {
				return errors.New("steer mismatch")
			}
			steerID = m.ID
			send(fields{"id": "approval-1", "method": "item/fileChange/requestApproval", "params": fields{"threadId": "thread", "reason": "fixture"}})
			continue
		case "":
			switch string(m.ID) {
			case `"approval-1"`:
				if !strings.Contains(string(m.Result), `"accept"`) {
					return errors.New("expected one-time approval")
				}
				send(fields{"id": 42, "method": "item/permissions/requestApproval", "params": fields{"threadId": "thread", "permissions": fields{"network": fields{"enabled": true}}}})
				continue
			case "42":
				if !strings.Contains(string(m.Result), `"permissions":{}`) {
					return errors.New("expected denial")
				}
				send(fields{"id": 43, "method": "item/commandExecution/requestApproval", "params": fields{"threadId": "thread"}})
				continue
			case "43":
				if !strings.Contains(string(m.Result), `"decline"`) {
					return errors.New("expected timeout denial")
				}
				if err := writeFile("three"); err != nil {
					return err
				}
				ctx, cancel := context.WithTimeout(context.Background(), fixtureLimit)
				cmd := exec.CommandContext(ctx, os.Args[0], "check-files")
				err := cmd.Run()
				cancel()
				if err != nil {
					return err
				}
				itemEvent(fields{"type": "commandExecution", "status": "completed", "command": "check-files", "exitCode": cmd.ProcessState.ExitCode()})
				notify("turn/diff/updated", fields{"turnId": turnID, "diff": "--- /dev/null\n+++ b/three\n@@ -0,0 +1 @@\n+three\n"})
				notify("thread/tokenUsage/updated", fields{"turnId": turnID, "tokenUsage": fields{"total": fields{"totalTokens": 8}}})
				itemEvent(fields{"type": "agentMessage", "text": "STATUS: DONE\nthree files"})
				notify("turn/completed", fields{"turn": fields{"id": turnID, "status": "completed", "items": []item{}}})
				send(fields{"id": steerID, "result": fields{"turnId": turnID}})
				continue
			default:
				return fmt.Errorf("unexpected reply: %s", m.ID)
			}
		case "turn/interrupt":
			notify("turn/completed", fields{"turn": fields{"id": turnID, "status": "interrupted"}})
		default:
			return fmt.Errorf("unexpected method %s", m.Method)
		}
		send(fields{"id": m.ID, "result": result})
	}
}

func waitFixtureStart(ctx context.Context) error {
	for {
		if exists, err := fileExists(os.Getenv("FIX_WAIT_START")); exists || err != nil {
			return err
		}
		if err := pause(ctx, 10*time.Millisecond); err != nil {
			return err
		}
	}
}

func runnerFixture(t *testing.T) (root, work, prompt string) {
	t.Helper()
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	tmp := t.TempDir()
	work = filepath.Join(tmp, "work dir")
	if err := os.Mkdir(work, 0700); err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	name := "codex"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	if err = os.WriteFile(filepath.Join(tmp, name), data, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tmp+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("HOME", tmp)
	t.Setenv("USERPROFILE", tmp)
	prompt = filepath.Join(tmp, "prompt")
	if err = os.WriteFile(prompt, []byte("task"), 0600); err != nil {
		t.Fatal(err)
	}
	root = filepath.Join(tmp, "state")
	setTestHost(t, root, nil, "")
	t.Setenv("CXRUN_STATE", root)
	t.Setenv("CXRUN_PREAMBLE_FILE", "")
	return
}

func TestRunnerConversation(t *testing.T) {
	root, work, prompt := runnerFixture(t)
	var err error
	dir := filepath.Join(root, "job")
	ctx, cancel := fixtureContext(t)
	defer cancel()
	clock := timeshim.NewFake(time.Now())
	done := make(chan error, 1)
	go func() {
		done <- run(runnerClockContext(ctx, clock), []string{"start", "--job", "job", "--dir", work, "--prompt-file", prompt, "--sandbox", "workspace-write", "--network", "--approval-timeout", "1200ms", "--idle-exit", "2m"}) // ended by close below, not by idle timing
	}()
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	until := func(want func(status) bool) status {
		t.Helper()
		s, waitErr := awaitFixtureStatus(ctx, dir, want)
		check(waitErr)
		return s
	}
	until(func(s status) bool { return s.Turn == "first" })
	check(run(ctx, []string{"start", "--job", "job", "--prompt-file", "missing"}))
	check(run(ctx, []string{"resume", "--job", "job"}))
	check(control(ctx, "say", dir, []string{"add three"}))
	approvalFile := func(which, id string) {
		t.Helper()
		until(func(s status) bool {
			return s.State == "waiting_approval" && len(s.Approvals) == 1 && string(s.Approvals[0].ID) == id
		})
		check(run(ctx, []string{which, "--job", "job", id}))
	}
	approvalFile("approve", `"approval-1"`)
	approvalFile("deny", "42")
	until(func(s status) bool { return len(s.Approvals) == 1 && string(s.Approvals[0].ID) == "43" })
	clock.Advance(1200 * time.Millisecond)
	until(func(s status) bool { return s.State == "waiting" && s.Finished == 1 && len(s.Usage) > 0 })
	check(control(ctx, "wait", dir, nil))
	for _, p := range []string{"one", "two", "three"} {
		b, e := os.ReadFile(filepath.Join(work, p))
		check(e)
		if string(b) != p {
			t.Fatal(p, string(b))
		}
	}
	b, err := readTurnReport(ctx, dir, "first")
	check(err)
	if string(b) != "STATUS: DONE\nthree files" {
		t.Fatal(string(b))
	}
	check(control(ctx, "say", dir, []string{"second round"}))
	until(func(s status) bool { return s.Turn == "second" && s.State == "running" })
	check(run(ctx, []string{"interrupt", "--job", "job"}))
	until(func(s status) bool { return s.CompletedStatus == "interrupted" })
	check(run(ctx, []string{"close", "--job", "job"}))
	select {
	case err = <-done:
		check(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var s status
	check(loadJobStatusInto(dir, &s))
	if s.State != "done" || s.Finished != 2 || s.PID != 0 {
		t.Fatal(s)
	}
	b, err = os.ReadFile(filepath.Join(dir, "progress.log"))
	check(err)
	for _, needle := range []string{"turn/steer", "fileChange\tcompleted add three", "exit=0 completed check-files", "+1 -0 three", "allow=false timeout", "turn/plan/updated", "second interrupted"} {
		if !strings.Contains(string(b), needle) {
			t.Error("missing progress entry", needle)
		}
	}
	events, err := os.Open(filepath.Join(dir, "events.jsonl"))
	check(err)
	defer func() { _ = events.Close() }()
	scanner := bufio.NewScanner(events)
	var count int
	for scanner.Scan() {
		var m message
		check(json.Unmarshal(scanner.Bytes(), &m))
		if m.Method == "" {
			t.Fatal("unframed event")
		}
		count++
	}
	check(scanner.Err())
	if count < 15 {
		t.Fatal("missing raw events", count)
	}
}

func TestConcurrentRegistrationAndInterruptedDelivery(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "job")
	results := make(chan error, 2)
	for _, prompt := range []string{"one", "two"} {
		go func(p string) {
			results <- register(root, dir, config{Prompt: p}, status{Job: "job", State: "running"})
		}(prompt)
	}
	a, b := <-results, <-results
	if (a == nil) == (b == nil) {
		t.Fatalf("expected exactly one publisher: %v / %v", a, b)
	}
	var c config
	if err := readJobConfig(dir, &c); err != nil || (c.Prompt != "one" && c.Prompt != "two") {
		t.Fatal(c, err)
	}
	log, err := os.OpenFile(filepath.Join(dir, "progress.log"), os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	r := runner{dir: dir, log: log, c: c}
	startFixtureWriter(t, &r)
	if err = r.refreshInbox(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ack.msg", "unknown.msg"} {
		record, recordErr := newRecord("msg", inboxMessage{Text: "same"})
		if recordErr != nil {
			t.Fatal(recordErr)
		}
		record.ID = name
		record, recordErr = appendInbox(t.Context(), dir, record, nil)
		if recordErr != nil {
			t.Fatal(recordErr)
		}
		if claimErr := r.claimInbox(record, "deliver"); claimErr != nil {
			t.Fatal(claimErr)
		}
	}
	if err = r.recoverInbox(turn{Items: []item{{Type: "userMessage", ClientID: "ack.msg"}}}); err != nil {
		t.Fatal(err)
	}
	in, err := readInboxState(dir)
	if err != nil || in.acks[1].Result != "delivered" || in.acks[2].Result != "uncertain" || len(in.claims) != 0 {
		t.Fatal(in, err)
	}
	for _, name := range []string{"ack.msg", "unknown.msg"} {
		if publishErr := publishMessage(t.Context(), dir, name, inboxMessage{Text: "same"}); publishErr != nil {
			t.Fatal(publishErr)
		}
	}
	pending, err := pendingInboxMessages(dir)
	if err != nil || len(pending) != 0 {
		t.Fatal(pending, err)
	}
}

func TestWaitKeepsOriginalTurn(t *testing.T) {
	dir := fixture(t, "waiting")
	o, err := openJobObservation(dir, stateCursor{}, "")
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(o)
	o.floor = &status{Finished: 1, Turn: "target", TurnStatus: "inProgress"}
	r := runner{dir: dir}
	startFixtureWriter(t, &r)
	for _, s := range []status{
		{Finished: 2, LastCompleted: "target", CompletedStatus: "completed"},
		{Finished: 2, LastCompleted: "target", CompletedStatus: "interrupted"},
		{Finished: 3, LastCompleted: "next", CompletedStatus: "completed"},
	} {
		if err := r.stateLog.Append("status", s); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.read(); err != nil {
		t.Fatal(err)
	}
	if sel := o.selected; sel == nil || sel.TurnID != "target" || sel.Outcome != "interrupted" {
		t.Fatal(sel)
	}
}

type testRuns struct {
	ctx     context.Context
	t       *testing.T
	runs    map[string]chan error
	clocks  map[string]*timeshim.Fake
	root    string
	work    string
	prompt  string
	workers sync.WaitGroup
}

func newRuns(t *testing.T, mode string) *testRuns {
	root, work, prompt := runnerFixture(t)
	t.Setenv("FIXMODE", mode)
	ctx, cancel := fixtureContext(t)
	h := &testRuns{t: t, ctx: ctx, runs: map[string]chan error{}, clocks: map[string]*timeshim.Fake{}, root: root, work: work, prompt: prompt}
	t.Cleanup(func() {
		cancel()
		done := make(chan struct{})
		go func() {
			h.workers.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(fixtureLimit):
			t.Error("fixture runners did not stop after cancellation")
		}
	})
	return h
}
func (h *testRuns) args(id string, extra ...string) []string {
	return append([]string{"start", "--job", id, "--dir", h.work, "--prompt-file", h.prompt, "--sandbox", "workspace-write", "--network", "--idle-exit", "2m"}, extra...)
}
func (h *testRuns) start(id string, extra ...string) <-chan error {
	done := make(chan error, 1)
	h.runs[id] = done
	ctx := h.ctx
	if c := h.clocks[id]; c != nil {
		ctx = runnerClockContext(ctx, c)
	}
	h.workers.Go(func() { done <- run(ctx, h.args(id, extra...)) })
	return done
}
func (h *testRuns) await(id string, match func(status) bool) status {
	h.t.Helper()
	s, err := awaitFixtureStatus(h.ctx, filepath.Join(h.root, id), match)
	if err != nil {
		for job, done := range h.runs {
			select {
			case e := <-done:
				h.t.Logf("runner %s already returned: %v", job, e)
				done <- e
			default:
			}
		}
		h.t.Fatalf("%s: %+v: %v", id, s, err)
	}
	return s
}
func (h *testRuns) ctl(op, id string, opts ...string) error {
	return run(h.ctx, append([]string{op, "--job", id}, opts...))
}

func TestFirstTurnPreamble(t *testing.T) {
	for _, tc := range []struct {
		name, want, explicit string
		useEnv, useExplicit  bool
	}{
		{name: "none", want: "task"},
		{name: "environment", useEnv: true, want: "环境前言\n\ntask"},
		{name: "explicit takes precedence", useEnv: true, useExplicit: true, explicit: "指定前言\n", want: "指定前言\n\ntask"},
		{name: "explicit empty file", useEnv: true, useExplicit: true, want: "task"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRuns(t, "complete")
			t.Setenv("FIXPROMPT", tc.want)
			dir := t.TempDir()
			if tc.useEnv {
				path := filepath.Join(dir, "env preamble")
				if err := os.WriteFile(path, []byte("环境前言\r\n"), 0600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("CXRUN_PREAMBLE_FILE", path)
			}
			var opts []string
			if tc.useExplicit {
				path := filepath.Join(dir, "explicit preamble")
				if err := os.WriteFile(path, []byte(tc.explicit), 0600); err != nil {
					t.Fatal(err)
				}
				opts = []string{"--preamble-file", path}
			}
			if err := h.finishFirstTurn("job", opts...); err != nil {
				stderr, _ := os.ReadFile(filepath.Join(h.root, "job", "server.stderr.log"))
				t.Fatalf("runner: %v\n%s", err, stderr)
			}
			var saved config
			if err := readJobConfig(filepath.Join(h.root, "job"), &saved); err != nil || saved.Prompt != tc.want {
				t.Fatalf("saved prompt: %q, err: %v", saved.Prompt, err)
			}
		})
	}
}

func TestInvalidMessageFile(t *testing.T) {
	h := newRuns(t, "complete")
	path := filepath.Join(t.TempDir(), "message")
	if err := os.WriteFile(path, []byte{0xff}, 0600); err != nil {
		t.Fatal(err)
	}
	for _, flag := range []string{"--preamble-file", "--stall-nudge-file"} {
		if err := run(h.ctx, h.args("job", flag, path)); err == nil || !strings.Contains(err.Error(), "expected UTF-8") {
			t.Fatalf("%s invalid text: %v", flag, err)
		}
		if err := run(h.ctx, h.args("job", flag, path+".missing")); !os.IsNotExist(err) {
			t.Fatalf("%s missing file: %v", flag, err)
		}
	}
	if _, err := os.Stat(filepath.Join(h.root, "job")); !os.IsNotExist(err) {
		t.Fatalf("invalid input registered a job: %v", err)
	}
}

func TestDangerFullAccess(t *testing.T) {
	h := newRuns(t, "exit")
	t.Setenv("FIXSANDBOX", "danger-full-access")
	const id = "privileged"
	h.withClock(id)
	done := h.start(id, "--sandbox", "danger-full-access")
	if err := h.idleExit(id, done, func(s status) bool {
		return s.State == "waiting" && s.LastCompleted == "continued"
	}); err != nil {
		stderr, _ := os.ReadFile(filepath.Join(h.root, id, "server.stderr.log"))
		t.Fatalf("runner: %v\n%s", err, stderr)
	}
	dir := filepath.Join(h.root, id)
	var s status
	if err := loadJobStatusInto(dir, &s); err != nil || !s.Privileged || s.LastCompleted != "continued" || s.CompletedStatus != "completed" || s.Restarts != 1 {
		t.Fatalf("status: %+v, err: %v", s, err)
	}
	f, err := os.Open(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	scan := bufio.NewScanner(f)
	started := 0
	for scan.Scan() {
		var record struct {
			Method     string
			Privileged bool
		}
		if err := json.Unmarshal(scan.Bytes(), &record); err != nil || !record.Privileged {
			t.Fatalf("event: %s, err: %v", scan.Bytes(), err)
		}
		if record.Method == "turn/started" {
			started++
		}
	}
	if err := scan.Err(); err != nil || started != 2 {
		t.Fatalf("started events: %d, err: %v", started, err)
	}
}

func TestStallAndCrashRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, mode, nudge string
	}{
		{name: "silent", mode: "silent"},
		{name: "exit", mode: "exit"},
		{name: "silent custom", mode: "silent", nudge: "检查进度并继续。\n"},
		{name: "exit custom", mode: "exit", nudge: "检查进度并继续。\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mode := tc.mode
			h := newRuns(t, mode)
			id := "p/recover"
			clock := h.withClock(id)
			opts := []string{"--stall", "200ms", "--idle-exit", "100ms"}
			if tc.nudge != "" {
				path := filepath.Join(t.TempDir(), "stall nudge")
				if err := os.WriteFile(path, []byte(tc.nudge), 0600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("FIXNUDGE", tc.nudge)
				opts = append(opts, "--stall-nudge-file", path)
			}
			done := h.start(id, opts...)
			h.await(id, func(s status) bool { return s.Turn == "first" })
			if mode == "silent" {
				h.stallUntilRestart(id, clock)
				clock.Advance(time.Second)
			} else {
				h.await(id, func(s status) bool { return s.State == "waiting" && s.LastCompleted == "continued" })
				clock.Advance(time.Second)
			}
			waitErr := h.ctl("wait", id, "--timeout", "2m")
			err := <-done
			var want error
			if mode == "silent" {
				want = errStalled
			}
			if !errors.Is(err, want) || (!errors.Is(waitErr, want) && (mode != "silent" || !errors.Is(waitErr, stateFailure("stalled")))) {
				t.Fatalf("runner=%v wait=%v", err, waitErr)
			}
			b, err := os.ReadFile(filepath.Join(h.root, id, "progress.log"))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Count(string(b), "\tthread/resume\t") != 1 || !strings.Contains(string(b), "continue-") {
				t.Fatal(string(b))
			}
			if mode == "silent" {
				for _, word := range []string{"step 1", "step 2", "step 3", "step 4"} {
					if !strings.Contains(string(b), word) {
						t.Fatal(string(b))
					}
				}
			}
		})
	}
}
func TestFIFOQueuedCloseAndDedup(t *testing.T) {
	h := newRuns(t, "silent")
	setTestHost(t, h.root, map[string]int{"agent": 1}, "")
	g, err := registryLock(h.ctx, h.root)
	if err != nil {
		t.Fatal(err)
	}
	if closeErr := g.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	held, err := lockPath(filepath.Join(h.root, ".admit", "slot-0.lock"), os.O_CREATE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	queued := func(s status) bool { return s.State == "queued" && s.PID != 0 }
	first := h.start("alpha/j", "--dedup-key", "abc")
	h.await("alpha/j", queued)
	clock := timeshim.NewFake(time.Now())
	waited := make(chan error, 1)
	go func() {
		waited <- run(clockContext(h.ctx, clock), []string{"wait", "--job", "alpha/j", "--timeout", "1s"})
	}()
	if err = clock.WaitDeadline(h.ctx, clock.Now().Add(200*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	if err = <-waited; !errors.Is(err, errObserveTimeout) {
		t.Fatal(err)
	}
	if err = h.ctl("start", "alpha/j"); err != nil {
		t.Fatal(err)
	}
	if err = run(h.ctx, h.args("beta/j", "--dedup-key", "abc")); !errors.Is(err, errDuplicate) {
		t.Fatal(err)
	}
	second := h.start("beta/j")
	h.await("beta/j", queued)
	third := h.start("gamma/j")
	h.await("gamma/j", queued)
	if err = h.ctl("close", "gamma/j"); err != nil {
		t.Fatal(err)
	}
	if err = <-third; err != nil {
		t.Fatal(err)
	}
	if closeErr := held.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	h.await("alpha/j", func(s status) bool { return s.Turn == "first" })
	var s status
	if err = loadJobStatusInto(filepath.Join(h.root, "beta", "j"), &s); err != nil || !queued(s) {
		t.Fatal(s, err)
	}
	if err = h.ctl("clean", "alpha/j"); err == nil {
		t.Fatal("cleaned active job")
	}
	if err = h.ctl("close", "alpha/j"); err != nil {
		t.Fatal(err)
	}
	if err = <-first; err != nil {
		t.Fatal(err)
	}
	h.await("beta/j", func(s status) bool { return s.Turn == "first" })
	if err = h.ctl("close", "beta/j"); err != nil {
		t.Fatal(err)
	}
	if err = <-second; err != nil {
		t.Fatal(err)
	}
}
func TestProjectsRunConcurrently(t *testing.T) {
	h := newRuns(t, "silent")
	setTestHost(t, h.root, map[string]int{"agent": 2}, "")
	var ends []<-chan error
	for _, id := range []string{"a/same", "b/same"} {
		ends = append(ends, h.start(id, "--session", id))
	}
	for _, id := range []string{"a/same", "b/same"} {
		s := h.await(id, func(s status) bool { return s.Turn == "first" })
		if s.Session != id {
			t.Fatal(s)
		}
	}
	for i, id := range []string{"a/same", "b/same"} {
		if err := h.ctl("close", id); err != nil {
			t.Fatal(err)
		}
		if err := <-ends[i]; err != nil {
			t.Fatal(err)
		}
	}
}
func TestCleanKeepsSuccessfulReceipt(t *testing.T) {
	h := newRuns(t, "complete")
	if err := h.finishFirstTurn("p/a", "--dedup-key", "key"); err != nil {
		t.Fatal(err)
	}
	if err := h.ctl("clean", "p/a"); err != nil {
		t.Fatal(err)
	}
	if err := run(h.ctx, h.args("p/b", "--dedup-key", "key")); !errors.Is(err, errDuplicate) {
		t.Fatal(err)
	}
	if err := h.finishFirstTurn("p/b", "--dedup-key", "key", "--force"); err != nil {
		t.Fatal(err)
	}
}

func TestLockedRegisteredJob(t *testing.T) {
	root := t.TempDir()
	t.Setenv("CXRUN_STATE", root)
	dir := filepath.Join(root, "legacy")
	if err := register(root, dir, config{Prompt: "saved"}, status{Job: "legacy", State: "running"}); err != nil {
		t.Fatal(err)
	}
	_, before, err := latestState(dir)
	if err != nil {
		t.Fatal(err)
	}
	held, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || held == nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	ctx, cancel := fixtureContext(t)
	defer cancel()
	for _, op := range []string{"start", "resume", "status"} {
		if err = run(ctx, []string{op, "--job", "legacy"}); err != nil {
			t.Fatal(err)
		}
	}
	clock := timeshim.NewFake(time.Now())
	closeCtx, stopClose := timeshim.WithTimeout(clockContext(ctx, clock), clock, time.Second)
	defer stopClose()
	done := make(chan error, 1)
	go func() { done <- run(closeCtx, []string{"close", "--job", "legacy"}) }()
	if waitErr := clock.WaitDeadline(ctx, clock.Now().Add(50*time.Millisecond)); waitErr != nil {
		t.Fatal(waitErr)
	}
	clock.Advance(time.Second)
	if err = <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close returned without a final record: %v", err)
	}
	if err = run(ctx, []string{"clean", "--job", "legacy"}); err == nil {
		t.Fatal("clean accepted live owner")
	}
	_, after, err := latestState(dir)
	if err != nil || after != before {
		t.Fatalf("modified locked status: %v -> %v (%v)", before, after, err)
	}
}

func TestListAndWaitCLI(t *testing.T) {
	root := t.TempDir()
	t.Setenv("CXRUN_STATE", root)
	for _, s := range []status{
		{Job: "legacy", State: "failed"},
		{Project: "p", Job: "same", Session: "s", State: "done", CompletedStatus: "completed"},
		{Project: "q", Job: "same", Session: "s", State: "queued"},
		{Project: "p", Job: "other", Session: "other", State: "stalled"},
	} {
		parent := filepath.Join(root, s.Project)
		if err := os.MkdirAll(parent, 0700); err != nil {
			t.Fatal(err)
		}
		if err := register(parent, filepath.Join(parent, s.Job), config{}, s); err != nil {
			t.Fatal(err)
		}
	}
	lines := fixtureCLI(t, 0, "list", "--project", "p", "--session", "s")
	var s status
	if err := json.Unmarshal([]byte(lines), &s); err != nil || s.Project != "p" || s.Job != "same" {
		t.Fatal(lines, err)
	}
	if n := strings.Count(fixtureCLI(t, 0, "list"), "\n"); n != 4 {
		t.Fatal("expected four jobs", n)
	}
	fixtureCLI(t, 0, "wait", "--job", "p/same")
	fixtureCLI(t, 1, "wait", "--job", "legacy")
	fixtureCLI(t, 3, "wait", "--job", "p/other")
	fixtureCLI(t, 5, "wait", "--job", "q/same", "--timeout", "2m")
}

func fixtureCLI(t *testing.T, expected int, args ...string) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := fixtureContext(t)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, append([]string{"fixture-cli"}, args...)...)
	b, err := cmd.CombinedOutput()
	if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != expected {
		t.Fatalf("%v: %v: %s", args, err, b)
	}
	return string(b)
}

func TestLoadGateAndAbandonedTicket(t *testing.T) {
	h := newRuns(t, "complete")
	setTestHost(t, h.root, map[string]int{"agent": 1}, "exit 1")
	// Kill only this CLI child. The failing load probe keeps it in the queue
	// without an app-server, and the next runner must reclaim the stale ticket.
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	args := h.args("p/abandoned")
	cmd := exec.CommandContext(h.ctx, exe, append([]string{"fixture-cli"}, args...)...)
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	h.await("p/abandoned", func(s status) bool { return s.State == "queued" && s.PID == cmd.Process.Pid })
	h.await("p/abandoned", func(s status) bool { return s.QueueReason != "" })
	if err = h.ctl("wait", "p/abandoned", "--timeout", "2m"); !errors.Is(err, ctlExit(exitQueued)) {
		t.Fatal(err)
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err = cmd.Wait(); err == nil {
		t.Fatal("expected killed runner")
	}
	setTestHost(t, h.root, map[string]int{"agent": 1}, "exit 0")
	if err = h.finishFirstTurn("p/next"); err != nil {
		t.Fatal(err)
	}
}

// Run three interactive servers against one slot. The follow-up to A must
// queue behind C, even though A's server and thread already exist.
func TestTurnSlotHandoff(t *testing.T) {
	h := newRuns(t, "manual")
	setTestHost(t, h.root, map[string]int{"agent": 1}, "")
	queued := func(s status) bool { return s.State == "queued" && s.PID != 0 }
	first := func(s status) bool { return s.State == "running" && s.Turn == "first" }
	submit := func(id string) <-chan error { return h.start(id, "--idle-exit", "1m") }
	ctl := func(op, id string, args ...string) {
		t.Helper()
		if err := h.ctl(op, id, args...); err != nil {
			t.Fatal(err)
		}
	}
	a := submit("a")
	original := h.await("a", first)
	b := submit("b")
	h.await("b", queued)
	ctl("say", "a", "completed")
	h.await("a", func(s status) bool { return s.State == "waiting" && s.CompletedStatus == "completed" })
	h.await("b", first)
	c := submit("c")
	h.await("c", queued)
	ctl("say", "a", "second round")
	waiting := h.await("a", queued)
	if waiting.EnginePID != original.EnginePID || waiting.Turn != "first" {
		t.Fatal(waiting)
	}
	ctl("interrupt", "b")
	h.await("c", first)
	var s status
	if err := loadJobStatusInto(filepath.Join(h.root, "a"), &s); err != nil || !queued(s) {
		t.Fatal("follow-up overtook c", s, err)
	}
	ctl("say", "c", "failed")
	second := h.await("a", func(s status) bool { return s.Turn == "second" && s.State == "running" })
	if second.EnginePID != original.EnginePID || second.Thread != original.Thread {
		t.Fatal(second)
	}
	for id, end := range map[string]<-chan error{"a": a, "b": b, "c": c} {
		ctl("close", id)
		if err := <-end; err != nil {
			t.Fatal(err)
		}
	}
}

func TestRestartKeepsActiveSlot(t *testing.T) {
	h := newRuns(t, "silent")
	setTestHost(t, h.root, map[string]int{"agent": 1}, "")
	clock := h.withClock("recover")
	recovery := h.start("recover", "--stall", "600ms")
	h.await("recover", func(s status) bool { return s.Turn == "first" })
	h.stallUntilRestart("recover", clock)
	next := h.start("next")
	h.await("next", func(s status) bool { return s.State == "queued" && s.QueueReason != "" })
	clock.Advance(time.Second)
	h.await("next", func(s status) bool { return s.Turn == "first" })
	var s status
	if err := loadJobStatusInto(filepath.Join(h.root, "recover"), &s); err != nil || s.State != "stalled" {
		t.Fatalf("competitor entered during restart: %+v %v", s, err)
	}
	if err := <-recovery; !errors.Is(err, errStalled) {
		t.Fatal(err)
	}
	if err := h.ctl("close", "next"); err != nil {
		t.Fatal(err)
	}
	if err := <-next; err != nil {
		t.Fatal(err)
	}
}

func TestFollowupQueueDeadlines(t *testing.T) {
	for _, expireQueue := range []bool{false, true} {
		t.Run(fmt.Sprint("expireQueue=", expireQueue), func(t *testing.T) {
			h := newRuns(t, "manual")
			setTestHost(t, h.root, map[string]int{"agent": 1}, "")
			h.t.Setenv("GORACE", "atexit_sleep_ms=0")
			const queueTimeout = 350 * time.Millisecond
			queueLimit := "0"
			if expireQueue {
				queueLimit = queueTimeout.String()
			}
			clock := h.withClock("a")
			a := h.start("a", "--idle-exit", "1m", "--timeout", "2s", "--queue-timeout", queueLimit)
			h.await("a", func(s status) bool { return s.Turn == "first" })
			b := h.start("b", "--idle-exit", "1m")
			h.await("b", func(s status) bool { return s.State == "queued" && s.PID != 0 })
			if err := h.ctl("say", "a", "completed"); err != nil {
				t.Fatal(err)
			}
			h.await("b", func(s status) bool { return s.Turn == "first" })
			if err := h.ctl("say", "a", "second round"); err != nil {
				t.Fatal(err)
			}
			h.await("a", func(s status) bool { return s.State == "queued" })
			wantState := "timeout"
			wantErr := error(context.DeadlineExceeded)
			if expireQueue {
				if err := clock.WaitDeadline(h.ctx, clock.Now().Add(queueTimeout)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(queueTimeout)
				wantState, wantErr = "queue_timeout", errQueueTimeout
				h.await("a", func(s status) bool { return s.State == wantState })
			} else {
				clock.Advance(3 * time.Second)
			}
			if err := h.ctl("close", "b"); err != nil {
				t.Fatal(err)
			}
			if err := <-b; err != nil {
				t.Fatal(err)
			}
			if !expireQueue {
				h.await("a", func(s status) bool { return s.Turn == "second" && s.State == "running" })
				clock.Advance(2 * time.Second)
				h.await("a", func(s status) bool { return s.State == "timeout" })
			}
			if err := <-a; !errors.Is(err, wantErr) || !expireQueue && errors.Is(err, errQueueTimeout) {
				t.Fatal(err)
			}
			if err := h.ctl("wait", "a"); !errors.Is(err, stateFailure(wantState)) {
				t.Fatal(err)
			}
		})
	}
}
