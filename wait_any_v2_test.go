package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

func TestWaitAnySinceFileStartsNow(t *testing.T) {
	ctx, cancel := fixtureContext(t)
	defer cancel()
	root := t.TempDir()
	r := waitFixture(t, root, "p/job", status{Project: "p", Job: "job", Session: "manager",
		RunID: "run", Engine: "codex", State: "waiting", Finished: 1, Turn: "one", LastCompleted: "one", CompletedStatus: "completed"})
	identity, _, _, identityErr := captureJobIdentity(r.dir)
	if identityErr != nil {
		t.Fatal(identityErr)
	}
	mail := agentMail{Type: "send", To: "parent", Text: "before the cursor"}
	if err := appendMail(ctx, r.dir, identity, "old-mail", &mail); err != nil {
		t.Fatal(err)
	}
	if err := r.begin("two"); err != nil {
		t.Fatal(err)
	}
	queued := waitFixture(t, root, "p/queued", status{Project: "p", Job: "queued", Session: "manager",
		RunID: "queue", Engine: "cmd", State: "queued"})
	if err := queued.setQueueReason("fixture blocked"); err != nil {
		t.Fatal(err)
	}
	terminal := waitFixture(t, root, "p/terminal", status{Project: "p", Job: "terminal", Session: "manager",
		RunID: "terminal", Engine: "cmd", State: "done"})
	if err := terminal.saveTerminal(); err != nil {
		t.Fatal(err)
	}
	question := waitFixture(t, root, "p/question", status{Project: "p", Job: "question", Session: "manager",
		RunID: "question", Engine: "codex", State: "waiting_input", InputKind: "next_turn", Finished: 1, LastCompleted: "one"})
	file := filepath.Join(t.TempDir(), "wait.json")
	start := time.Now()
	clock := timeshim.NewFake(start)
	bounded, stop := timeshim.WithTimeout(clockContext(ctx, clock), clock, 2*time.Second)
	defer stop()
	done := make(chan error, 1)
	var out bytes.Buffer
	go func() { done <- waitAny(bounded, root, []string{"p"}, "manager", file, false, &out) }()
	if err := clock.WaitDeadline(ctx, start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	select {
	case err := <-done:
		if !errors.Is(err, errObserveTimeout) || out.Len() != 0 {
			t.Fatalf("new cursor reported historical events: %s (%v)", &out, err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var saved map[string]waitCursor
	if err := readJSON(file, &saved); err != nil || saved["p/job"].Finished != 1 || saved["p/job"].OutboxSeq != mail.Seq ||
		saved["p/queued"].QueueGeneration != queued.s.QueueGeneration || saved["p/terminal"].Terminal != waitTerminalKey(terminal.s) ||
		saved["p/question"].Question != questionKey(question.s) {
		t.Fatalf("initial cursor: %+v (%v)", saved, err)
	}

	log, logErr := openJobFile(filepath.Join(r.dir, "progress.log"), jobFileAppend)
	if logErr != nil {
		t.Fatal(logErr)
	}
	defer closeBestEffort(log)
	r.log = log
	if err := r.finish(turn{ID: "two", Status: "failed", Error: &turnError{RateLimited: true}}); err != nil {
		t.Fatal(err)
	}
	if err := r.begin("three"); err != nil {
		t.Fatal(err)
	}
	if err := r.finish(turn{ID: "three", Status: "failed", Error: &turnError{Message: "unrelated failure"}}); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []struct {
		number int64
		rate   bool
	}{{2, true}, {3, false}} {
		out.Reset()
		if err := waitAny(ctx, root, []string{"p"}, "manager", file, true, &out); err != nil {
			t.Fatal(err)
		}
		var got waitReport
		if err := json.Unmarshal(out.Bytes(), &got); err != nil || got.Turn != expected.number || got.RateLimited != expected.rate ||
			got.Outcome != "failed" || got.Code != 1 || got.Next != fmt.Sprintf("cx result local:p/job --turn %d", expected.number) {
			t.Fatalf("turn %d event: %s (%v)", expected.number, &out, err)
		}
	}
}

func TestWaitAnyNamedCursorHistoryAndDuplicate(t *testing.T) {
	f := newControllerFixture(t)
	root := os.Getenv("CXRUN_STATE")
	add := func(p, session, tag string, count int) {
		t.Helper()
		r := waitFixture(t, root, p+"/"+tag, status{Project: p, Job: tag, Session: session, RunID: p + "-" + tag,
			Engine: "codex", State: "waiting"})
		for i := 1; i <= count; i++ {
			r.s.Finished = int64(i)
			r.s.LastCompleted, r.s.CompletedStatus = fmt.Sprint(i), "completed"
			if err := r.save(); err != nil {
				t.Fatal(err)
			}
		}
	}
	add("a", "manager", "excluded", 1)
	add("p", "other", "excluded", 1)
	add("p", "manager", "job", 2)
	add("q", "manager", "job", 1)
	t.Setenv("CX_SESSION", "")
	t.Setenv("OPENCODE_SESSION_ID", "manager")
	cursor, err := cursorFile("wait", "history", "sessions", snapSum([]byte("manager")), "cursor")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := registerWaitObserver(t.Context(), cursor+".lock", "history")
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(lease)
	for i, want := range []string{"cx result local:p/job --turn 1", "cx result local:p/job --turn 2", "cx result local:q/job --turn 1"} {
		args := []string{"wait", "--any", "--project", "p", "--project", "q", "--cursor", "history", "--timeout", "20s"}
		if i == 0 {
			args = append(args, "--from-start")
		}
		out, diag := cliStreams(t, 0, nil, f.cx, args...)
		var got waitReport
		if err := json.Unmarshal(out, &got); err != nil || got.Next != want || got.RateLimited || got.Host != "local" ||
			!strings.Contains(diag, "another process is waiting") {
			t.Fatalf("expected %s, got %s (%v), stderr %s", want, out, err, diag)
		}
	}
	// A separate named cursor writes the baseline via the real node wire.
	req := waitPollRequest{Projects: []string{"p", "q"}, Session: "manager", Limit: waitPollInterval}
	now := filepath.Join(filepath.Dir(cursor), "now.json")
	poll := jobWaitPoll(endpoint{State: root}, "local", now, req)
	if event, err := poll(t.Context()); err != nil || event.row != nil {
		t.Fatalf("new named cursor returned an event: %+v (%v)", event, err)
	}
	var baseline map[string]waitCursor
	if err := readJSON(now, &baseline); err != nil || len(baseline) != 3 || baseline["a/excluded"].Finished != 1 ||
		baseline["p/job"].Finished != 2 || baseline["q/job"].Finished != 1 {
		t.Fatalf("named baseline: %+v (%v)", baseline, err)
	}
}

func TestWaitAnyUnknownSession(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("CX_SESSION", "")
	out, diag := cliStreams(t, 2, nil, f.cx, "wait", "--any", "--timeout", "20s")
	if len(out) != 0 || !strings.Contains(diag, "set CX_SESSION or use --session or --all") {
		t.Fatalf("unknown session diagnostic: %s (%s)", out, diag)
	}
}

func TestWaitAnyHostRetryAndDeadline(t *testing.T) {
	for _, mode := range []string{"peer-ready", "recovery", "all-unreachable"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := fixtureContext(t)
			defer cancel()
			start := time.Unix(150, 0)
			clock := timeshim.NewFake(start)
			bounded, stop := timeshim.WithTimeout(clockContext(ctx, clock), clock, 10*time.Second)
			defer stop()
			calls := make(chan int, 4)
			peer := make(chan struct{})
			warnings := make(chan string, 5)
			dir := t.TempDir()
			first, second := filepath.Join(dir, "first.json"), filepath.Join(dir, "second.json")
			old := []byte("{}")
			newer := []byte(`{"newer":true}`)
			if err := atomicWrite(first, old); err != nil {
				t.Fatal(err)
			}
			n := 0
			sources := []*waitSource{
				{file: first, label: "first", jobHost: true, poll: func(context.Context) (waitDelivery, error) {
					n++
					calls <- n
					if mode == "recovery" && n == 3 {
						return waitDelivery{row: "recovered", before: old, cursor: newer}, nil
					}
					return waitDelivery{}, waitUnavailable{context.DeadlineExceeded}
				}},
				{file: second, label: "second", jobHost: true, poll: func(ctx context.Context) (waitDelivery, error) {
					if mode == "all-unreachable" {
						return waitDelivery{}, waitUnavailable{ctlExit(9)}
					}
					select {
					case <-peer:
						return waitDelivery{row: "healthy", cursor: old}, nil
					case <-ctx.Done():
						return waitDelivery{}, ctx.Err()
					}
				}},
			}
			var out bytes.Buffer
			done := make(chan error, 1)
			go func() {
				done <- waitForAny(bounded, sources, &out, func(format string, args ...any) {
					warnings <- fmt.Sprintf(format, args...)
				})
			}()
			nextCall := func(want int) {
				t.Helper()
				select {
				case got := <-calls:
					if got != want {
						t.Fatalf("attempt %d, want %d", got, want)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			nextCall(1)
			if err := clock.WaitDeadline(ctx, start.Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "peer-ready":
				close(peer)
			case "recovery":
				clock.Advance(time.Second)
				nextCall(2)
				if err := clock.WaitDeadline(ctx, start.Add(3*time.Second)); err != nil {
					t.Fatal(err)
				}
				clock.Advance(2 * time.Second)
				nextCall(3)
			case "all-unreachable":
				clock.Advance(10 * time.Second)
			}
			select {
			case err := <-done:
				switch mode {
				case "peer-ready":
					if err != nil || out.String() != "\"healthy\"\n" {
						t.Fatalf("healthy peer: %s (%v)", &out, err)
					}
				case "recovery":
					saved, readErr := readWaitCursorFile(first)
					if err != nil || out.String() != "\"recovered\"\n" || readErr != nil || !bytes.Equal(saved, newer) {
						t.Fatalf("recovered cursor: %s (%v), saved %s (%v)", &out, err, saved, readErr)
					}
				case "all-unreachable":
					if !errors.Is(err, ctlExit(124)) || out.Len() != 0 {
						t.Fatalf("offline deadline: %s (%v)", &out, err)
					}
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			close(warnings)
			var diag string
			for warning := range warnings {
				diag += warning
			}
			if strings.Count(diag, "first unavailable, retrying") != 1 ||
				strings.Contains(diag, "all selected hosts unavailable") != (mode == "all-unreachable") {
				t.Fatalf("unexpected warnings: %q", diag)
			}
		})
	}
}

func TestWaitAnyConcurrentCursorNeverRegresses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor.json")
	old, latest := []byte("{}"), []byte("{\"latest\":true}")
	if err := atomicWrite(path, old); err != nil {
		t.Fatal(err)
	}
	if _, advanced, err := updateWaitCursor(t.Context(), path, old, latest, nil); err != nil || !advanced {
		t.Fatal(err)
	}
	got, advanced, err := updateWaitCursor(t.Context(), path, old, []byte("{\"stale\":true}"), func() error {
		t.Fatal("emitted a stale event")
		return nil
	})
	if err != nil || advanced || !bytes.Equal(got, latest) {
		t.Fatalf("stale delivery moved cursor to %s (%v)", got, err)
	}
}

func TestWaitAnyRestoresDisconnectedNodeCursor(t *testing.T) {
	f := newControllerFixture(t)
	ctx, cancel := fixtureContext(t)
	defer cancel()
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	offline := filepath.Join(f.home, "offline")
	t.Setenv("CX_WAIT_FIXTURE_OFFLINE", offline)
	script := `#!/bin/sh
set -eu
while [ "$1" != -- ]; do shift; done
shift 2
if [ -f "$CX_WAIT_FIXTURE_OFFLINE" ]; then
  printf 'fixture disconnected\n' >&2
  exit 255
fi
exec /bin/sh -c "$*"
`
	if err := os.WriteFile(filepath.Join(f.home, "ssh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	remoteRoot := t.TempDir()
	r := waitFixture(t, remoteRoot, "p/job", status{Project: "p", Job: "job", Session: "manager", RunID: "remote",
		Engine: "cmd", State: "running"})
	file := filepath.Join(f.home, "worker.json")
	poll := jobWaitPoll(endpoint{SSH: "fixture", CX: f.cx, State: remoteRoot}, "worker", file,
		waitPollRequest{Session: "manager", Limit: waitPollInterval})
	if event, err := poll(ctx); err != nil || event.row != nil {
		t.Fatalf("initialize remote cursor: %+v (%v)", event, err)
	}
	if err := os.WriteFile(offline, nil, 0600); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	clock := timeshim.NewFake(start)
	bounded, stop := timeshim.WithTimeout(clockContext(ctx, clock), clock, 10*time.Second)
	defer stop()
	warnings := make(chan string, 3)
	done := make(chan error, 1)
	var out bytes.Buffer
	go func() {
		sources := []*waitSource{{poll: poll, file: file, label: "worker", jobHost: true}}
		done <- waitForAny(bounded, sources, &out, func(format string, args ...any) {
			warnings <- fmt.Sprintf(format, args...)
		})
	}()
	select {
	case warning := <-warnings:
		if !strings.Contains(warning, "worker unavailable, retrying") || !strings.Contains(warning, "fixture disconnected") {
			t.Fatal(warning)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := clock.WaitDeadline(ctx, start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	r.s.State, r.s.Finished, r.s.LastCompleted, r.s.CompletedStatus = "done", 1, "1", "completed"
	if err := r.saveTerminal(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(offline); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var row waitReport
	if err := json.Unmarshal(out.Bytes(), &row); err != nil || row.Next != "cx result worker:p/job --turn 1" {
		t.Fatalf("reconnected event: %s (%v)", &out, err)
	}
}

func TestWaitAnyFollowsAdoptionWithoutReplayingAcknowledgedEvents(t *testing.T) {
	f := newControllerFixture(t)
	ctx, cancel := fixtureContext(t)
	defer cancel()
	root := os.Getenv("CXRUN_STATE")
	dir := filepath.Join(root, "p", "job")
	if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
		t.Fatal(err)
	}
	s := status{Project: "p", Job: "job", Session: "source", RunID: "adoption", Engine: "cmd", State: "done",
		Finished: 1, Turn: "1", LastCompleted: "1", CompletedStatus: "completed"}
	if err := register(filepath.Dir(dir), dir, config{Project: "p", Engine: s.Engine, Session: s.Session, RunID: s.RunID}, s); err != nil {
		t.Fatal(err)
	}
	if err := writeFixtureStatus(dir, s); err != nil {
		t.Fatal(err)
	}
	cursorPath := func(session string) string {
		t.Helper()
		path, err := cursorFile("wait", "default", "sessions", snapSum([]byte(session)), "local.json")
		if err != nil {
			t.Fatal(err)
		}
		return path
	}
	wait := func(session string, extra ...string) waitReport {
		t.Helper()
		out := cliOutput(t, 0, nil, f.cx, append([]string{"wait", "--any", "--session", session, "--timeout", "20s"}, extra...)...)
		var row waitReport
		if err := json.Unmarshal(out, &row); err != nil || row.Event != "terminal" || row.EventID == "" || row.Turn != 1 {
			t.Fatalf("wait in %s: %s (%v)", session, out, err)
		}
		return row
	}
	before := wait("source", "--from-start")
	destination := cursorPath("destination")
	init := jobWaitPoll(endpoint{State: root}, "local", destination, waitPollRequest{Session: "destination", Limit: waitPollInterval})
	if event, err := init(ctx); err != nil || event.row != nil {
		t.Fatalf("initialize destination: %+v (%v)", event, err)
	}
	cliOutput(t, 0, nil, f.cx, "adopt", "local:p/job", "--session", "destination")
	for _, op := range []string{"ls", "overview"} {
		for _, session := range []string{"source", "destination"} {
			out := cliOutput(t, 0, nil, f.cx, op, "--session", session, "--json")
			if bytes.Contains(out, []byte(`"job":"job"`)) != (session == "destination") {
				t.Fatalf("%s in %s after adoption: %s", op, session, out)
			}
		}
	}
	after := wait("destination")
	if before.EventID != after.EventID {
		t.Fatalf("adoption changed event identity: %s to %s", before.EventID, after.EventID)
	}
	cliOutput(t, 0, nil, f.cx, "adopt", "local:p/job", "--session", "source")
	for _, session := range []string{"source", "destination"} {
		var saved map[string]waitCursor
		if err := readJSON(cursorPath(session), &saved); err != nil {
			t.Fatal(err)
		}
		assertNoWaitAnyEvent(t, ctx, root, "p", session, saved)
	}
}

func TestWaitAnyKeepsDeliveredPositionAcrossRunReplacement(t *testing.T) {
	for _, tc := range []struct {
		name, state, outcome string
		turn                 int64
		seed                 bool
	}{
		{"resumed-rate-limit", "running", "failed", 2, false},
		{"resumed-interrupt", "running", "interrupted", 1, false},
		{"repeated-third-turn", "waiting", "completed", 3, false},
		{"closed-after-completion", "done", "completed", 1, false},
		{"new-session-starts-now", "waiting", "completed", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := fixtureContext(t)
			defer cancel()
			root := t.TempDir()
			const id = "deploy-dev/ci-green"
			s := status{Project: "deploy-dev", Job: "ci-green", Session: "new-session", Engine: "codex", RunID: "original",
				State: "waiting", Finished: tc.turn, LastCompleted: "old-turn", CompletedStatus: tc.outcome,
				CompletedRateLimited: tc.name == "resumed-rate-limit"}
			r := waitFixture(t, root, id, s)
			projects := []string{s.Project}
			baseline, err := initialWaitBaseline(ctx, root, s.Session, !tc.seed)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.seed {
				row, pollErr := pollWaitAny(ctx, root, projects, s.Session, baseline)
				if pollErr != nil || row.Event != "completion" || row.Turn != tc.turn {
					t.Fatalf("initial delivery: %+v (%v)", row, pollErr)
				}
			}
			s.State = tc.state
			for _, run := range []string{"replacement", "replacement-again"} {
				s.RunID = run
				if writeErr := writeFixtureStatus(r.dir, s); writeErr != nil {
					t.Fatal(writeErr)
				}
				assertNoWaitAnyEvent(t, ctx, root, s.Project, s.Session, baseline)
			}
			s.Finished++
			s.LastCompleted, s.CompletedStatus, s.State = "new-turn", "completed", "waiting"
			s.CompletedRateLimited = false
			if err = writeFixtureStatus(r.dir, s); err != nil {
				t.Fatal(err)
			}
			row, err := pollWaitAny(ctx, root, projects, s.Session, baseline)
			if err != nil || row.Event != "completion" || row.Turn != s.Finished {
				t.Fatalf("completion after restart: %+v (%v)", row, err)
			}
		})
	}
}

func TestWaitAnyDeduplicatesWireReplay(t *testing.T) {
	f := newControllerFixture(t)
	ctx, cancel := fixtureContext(t)
	defer cancel()
	cursor := filepath.Join(t.TempDir(), "wait.json")
	replies := t.TempDir()
	t.Setenv("WAIT_REPLIES", replies)
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	script := `#!/bin/sh
set -eu
if [ "$1" = -G ]; then exit 1; fi
for phase in first offline replay next; do
  if [ -f "$WAIT_REPLIES/$phase" ]; then
    mv "$WAIT_REPLIES/$phase" "$WAIT_REPLIES/used-$phase"
    if [ "$phase" = offline ]; then exit 255; fi
    cat "$WAIT_REPLIES/used-$phase"
    exit 0
  fi
done
exit 1
`
	if err := os.WriteFile(filepath.Join(f.home, "ssh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	for i, phase := range []string{"first", "offline", "replay", "next"} {
		eventID, turn := "first-id", 1
		if phase == "next" {
			eventID, turn = "next-id", 2
		}
		reply := fmt.Sprintf(`{"row":{"project":"p","job":"job","event":"completion","event_id":%q,"turn":%d},"cursor":{"p/job":{"state_cursor":"1:0:%d","completed_turns":%d}}}`, eventID, turn, i+1, turn)
		if err := os.WriteFile(filepath.Join(replies, phase), []byte(reply), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := atomicWrite(cursor, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	source := func() *waitSource {
		poll := jobWaitPoll(endpoint{SSH: "fixture", CX: f.cx}, "worker", cursor, waitPollRequest{Limit: waitPollInterval})
		return &waitSource{poll: poll, file: cursor, label: "worker", jobHost: true}
	}
	var out bytes.Buffer
	if err := waitForAny(ctx, []*waitSource{source()}, &out, func(string, ...any) {}); err != nil || !strings.Contains(out.String(), "first-id") {
		t.Fatalf("first delivery: %s (%v)", &out, err)
	}
	out.Reset()
	start := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := timeshim.NewFake(start)
	done := make(chan error, 1)
	go func() {
		done <- waitForAny(clockContext(ctx, clock), []*waitSource{source()}, &out, func(string, ...any) {})
	}()
	if err := clock.WaitDeadline(ctx, start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	select {
	case err := <-done:
		if err != nil || !strings.Contains(out.String(), "next-id") || strings.Contains(out.String(), "first-id") {
			t.Fatalf("delivery after reconnect: %s (%v)", &out, err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestWaitAnyTerminalStaysAcknowledgedInSameDirectory(t *testing.T) {
	ctx, cancel := fixtureContext(t)
	defer cancel()
	root := t.TempDir()
	s := status{Project: "p", Job: "job", RunID: "first", Engine: "cmd", State: "done"}
	const id = "p/job"
	dir, err := jobDir(root, id)
	if err != nil {
		t.Fatal(err)
	}
	var baseline map[string]waitCursor
	if !t.Run("same directory", func(t *testing.T) {
		r := waitFixture(t, root, id, s)
		if err = r.saveTerminal(); err != nil {
			t.Fatal(err)
		}
		baseline, err = initialWaitBaseline(ctx, root, "", true)
		if err != nil {
			t.Fatal(err)
		}
		var row *waitReport
		row, err = pollWaitAny(ctx, root, nil, "", baseline)
		if err != nil || row.Event != "terminal" {
			t.Fatalf("initial terminal: %+v (%v)", row, err)
		}
		s.RunID = "second"
		if err = writeFixtureStatus(dir, s); err != nil {
			t.Fatal(err)
		}
		assertNoWaitAnyEvent(t, ctx, root, "p", "", baseline)
	}) {
		return
	}
	// The subtest has released the fixture owner. Keep the old directory
	// alive so its identity cannot be reused when re-registering.
	if err = os.Rename(dir, filepath.Join(root, ".old")); err != nil {
		t.Fatal(err)
	}
	// Even a reused run ID must not hide a newly registered directory.
	next := waitFixture(t, root, id, status{Project: "p", Job: "job", RunID: "first", Engine: "cmd", State: "done"})
	if err = next.saveTerminal(); err != nil {
		t.Fatal(err)
	}
	row, err := pollWaitAny(ctx, root, nil, "", baseline)
	if err != nil || row.Event != "terminal" {
		t.Fatalf("new directory terminal: %+v (%v)", row, err)
	}
}

func TestWaitAnyJobFiltersShareSessionCursor(t *testing.T) {
	f := newControllerFixture(t)
	root := os.Getenv("CXRUN_STATE")
	t.Setenv("CX_SESSION", "manager")
	for _, id := range []string{"p/a-excluded", "p/b-picked", "p/c-picked", "q/a-excluded"} {
		p, job, _ := strings.Cut(id, "/")
		r := waitFixture(t, root, id, status{Project: p, Job: job, Session: "manager", RunID: id, Engine: "cmd", State: "done"})
		if err := r.saveTerminal(); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range []string{"b-picked", "c-picked", "a-excluded"} {
		args := []string{"wait", "--any", "--project", "p", "--host", "local", "--timeout", "10s"}
		if i < 2 {
			args = append(args, "--job", "local:q/a-excluded", "--job", "local:p/b-picked", "--job=local:p/c-picked")
		}
		if i == 0 {
			args = append(args, "--from-start")
		}
		out := cliOutput(t, 0, nil, f.cx, args...)
		var row waitReport
		if err := json.Unmarshal(out, &row); err != nil || row.Job != want || row.Project != "p" {
			t.Fatalf("expected p/%s, got %s (%v)", want, out, err)
		}
	}
	for _, args := range [][]string{
		{"wait", "--any", "--job", "p/b-picked"},
		{"wait", "--any", "--job", "local:b-picked"},
		{"wait", "local:p/b-picked", "--job", "local:p/b-picked"},
	} {
		cliOutput(t, 2, nil, f.cx, args...)
	}
}

func TestWaitAnyInitializesUnselectedHost(t *testing.T) {
	f := newControllerFixture(t)
	root, remote := os.Getenv("CXRUN_STATE"), t.TempDir()
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CX_SESSION", "manager")
	t.Setenv("CX_HOSTS", "local,worker")
	config := fmt.Sprintf("[hosts.worker]\nssh='fixture'\ncx=%q\nstate=%q\n", f.cx, remote)
	if err := os.MkdirAll(filepath.Join(f.home, ".config", "cx"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.home, ".config", "cx", "hosts.toml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	script := `#!/bin/sh
set -eu
if [ "$1" = -G ]; then exit 1; fi
while [ "$1" != -- ]; do shift; done
shift 2
exec /bin/sh -c "$*"
`
	if err := os.WriteFile(filepath.Join(f.home, "ssh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	r := waitFixture(t, root, "p/job", status{Project: "p", Job: "job", Session: "manager",
		RunID: "local", Engine: "cmd", State: "done"})
	if err := r.saveTerminal(); err != nil {
		t.Fatal(err)
	}
	w := waitFixture(t, remote, "p/job", status{Project: "p", Job: "job", Session: "manager",
		RunID: "worker", Engine: "cmd", State: "running"})
	cliOutput(t, 0, nil, f.cx, "wait", "--any", "--job", "local:p/job", "--from-start", "--timeout", "20s")
	path, err := cursorFile("wait", "default", "sessions", snapSum([]byte("manager")), "cursor")
	if err != nil {
		t.Fatal(err)
	}
	var baseline map[string]waitCursor
	if err := readJSON(filepath.Join(filepath.Dir(path), "worker.json"), &baseline); err != nil || baseline["p/job"].RunID != "worker" {
		t.Fatalf("unselected host baseline: %+v (%v)", baseline, err)
	}
	w.s.State, w.s.Finished, w.s.LastCompleted, w.s.CompletedStatus = "done", 1, "one", "completed"
	if err := w.saveTerminal(); err != nil {
		t.Fatal(err)
	}
	out := cliOutput(t, 0, nil, f.cx, "wait", "--any", "--job", "worker:p/job", "--timeout", "20s")
	var row waitReport
	if err := json.Unmarshal(out, &row); err != nil || row.Host != "worker" || row.Turn != 1 {
		t.Fatalf("expected worker completion: %s (%v)", out, err)
	}
}

func TestWaitAnyNewSessionSeedsJobReplacedDuringBaseline(t *testing.T) {
	ctx, cancel := fixtureContext(t)
	defer cancel()
	root := t.TempDir()
	s := status{Project: "deploy-dev", Job: "ci-green", Session: "new-session", RunID: "first",
		Engine: "codex", State: "waiting", Finished: 1, LastCompleted: "old", CompletedStatus: "completed"}
	dir := filepath.Join(root, s.Project, s.Job)
	if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
		t.Fatal(err)
	}
	if err := register(filepath.Dir(dir), dir, config{RunID: s.RunID, Engine: s.Engine}, s); err != nil {
		t.Fatal(err)
	}
	guard, err := registryLock(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(guard)
	start := time.Unix(150, 0)
	clock := timeshim.NewFake(start)
	type initialResult struct {
		baseline map[string]waitCursor
		err      error
	}
	done := make(chan initialResult, 1)
	go func() {
		baseline, err := initialWaitBaseline(clockContext(ctx, clock), root, s.Session, false)
		done <- initialResult{baseline, err}
	}()
	// An unlocked owner makes the first status projection wait on our registry lock.
	if err := clock.WaitDeadline(ctx, start.Add(20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	s.RunID, s.State = "second", "done"
	if err := writeFixtureStatus(dir, s); err != nil {
		t.Fatal(err)
	}
	if err := guard.Close(); err != nil {
		t.Fatal(err)
	}
	clock.Advance(20 * time.Millisecond)
	select {
	case got := <-done:
		pos := got.baseline["deploy-dev/ci-green"]
		if got.err != nil || pos.Finished != 1 || pos.RunID != "second" {
			t.Fatalf("fresh session baseline: %+v (%v)", got.baseline, got.err)
		}
		assertNoWaitAnyEvent(t, ctx, root, s.Project, s.Session, got.baseline)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestWaitAnyRateLimitedResetAt(t *testing.T) {
	for _, tc := range []struct {
		name string
		want string
		used int
	}{
		{name: "blocking-window", used: 100, want: "110"},
		{name: "unrelated-window", used: 50, want: "null"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := fixtureContext(t)
			defer cancel()
			root := t.TempDir()
			r := waitFixture(t, root, "p/job", status{Project: "p", Job: "job", RunID: "run",
				Engine: "codex", State: "running", Turn: "one"})
			baseline, err := initialWaitBaseline(ctx, root, "", false)
			if err != nil {
				t.Fatal(err)
			}
			log, err := openJobFile(filepath.Join(r.dir, "progress.log"), jobFileAppend)
			if err != nil {
				t.Fatal(err)
			}
			defer closeBestEffort(log)
			r.log = log
			clock := timeshim.NewFake(time.Unix(100, 0))
			r.clock = clock
			r.s.Primary = &rateLimitWindow{UsedPercent: tc.used, ResetAt: new(int64(110))}
			r.s.Secondary = &rateLimitWindow{UsedPercent: 99, ResetAt: new(int64(105))}
			if err = r.finish(turn{ID: "one", Error: &turnError{RateLimited: true}}); err != nil {
				t.Fatal(err)
			}
			clock.Advance(20 * time.Second)
			r.s.Primary = &rateLimitWindow{UsedPercent: 100, ResetAt: new(int64(130))}
			if err = r.begin("two"); err != nil {
				t.Fatal(err)
			}
			report, err := pollWaitAny(clockContext(ctx, clock), root, nil, "", baseline)
			if err != nil {
				t.Fatal(err)
			}
			row, err := json.Marshal(report)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(row, &fields); err != nil || report.Turn != 1 || !report.RateLimited ||
				string(fields["reset_at"]) != tc.want {
				t.Fatalf("reset_at: %s (%v)", row, err)
			}
		})
	}
}

// Completions sealed while no observer runs must still be delivered when the
// job is already busy with a later turn at the next observation.
func TestWaitAnyDeliversCompletionsMissedWhileBusy(t *testing.T) {
	ctx, cancel := fixtureContext(t)
	defer cancel()
	root := t.TempDir()
	const id = "demo/long-run"
	s := status{Project: "demo", Job: "long-run", Session: "manager", Engine: "codex", RunID: "run",
		State: "waiting", Finished: 4, LastCompleted: "turn-4", CompletedStatus: "completed"}
	r := waitFixture(t, root, id, s)
	projects := []string{s.Project}
	baseline, err := initialWaitBaseline(ctx, root, s.Session, true)
	if err != nil {
		t.Fatal(err)
	}
	if row, err := pollWaitAny(ctx, root, projects, s.Session, baseline); err != nil || row.Turn != 4 {
		t.Fatalf("turn 4: %+v (%v)", row, err)
	}
	for turn := int64(5); turn <= 7; turn++ {
		s.Finished, s.LastCompleted, s.CompletedStatus = turn, fmt.Sprintf("turn-%d", turn), "completed"
		s.State, s.TurnStatus = "waiting", ""
		if err := writeFixtureStatus(r.dir, s); err != nil {
			t.Fatal(err)
		}
		s.State, s.TurnStatus, s.Turn = "running", "inProgress", fmt.Sprintf("turn-%d", turn+1)
		if err := writeFixtureStatus(r.dir, s); err != nil {
			t.Fatal(err)
		}
	}
	for turn := int64(5); turn <= 7; turn++ {
		row, err := pollWaitAny(ctx, root, projects, s.Session, baseline)
		if err != nil || row.Event != "completion" || row.Turn != turn {
			t.Fatalf("turn %d: %+v (%v)", turn, row, err)
		}
	}
	assertNoWaitAnyEvent(t, ctx, root, s.Project, s.Session, baseline)
}
