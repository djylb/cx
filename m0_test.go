package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/format"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

func TestStatesAndOrphanProjection(t *testing.T) {
	h := commands(t)
	var table map[string]int
	if err := json.Unmarshal([]byte(h.cli(0, "states")), &table); err != nil || !reflect.DeepEqual(table, map[string]int{"done": 0, "failed": 1, "stalled": 3, "timeout": 4, "queue_timeout": 4, "orphaned": 5}) {
		t.Fatal(table, err)
	}
	for _, op := range []string{"status", "list", "wait"} {
		dir := filepath.Join(h.root, op)
		if err := register(h.root, dir, config{Argv: []string{"never"}}, status{Job: op, Engine: "cmd", State: "queued"}); err != nil {
			t.Fatal(err)
		}
		switch op {
		case "list":
			h.cli(0, op)
		case "wait":
			h.cli(5, op, "--job", op)
		default:
			h.cli(0, op, "--job", op)
		}
		h.cli(5, "wait", "--job", op)
		b, err := os.ReadFile(filepath.Join(dir, "progress.log"))
		if err != nil || strings.Count(string(b), "\torphaned\t") != 1 {
			t.Fatal(string(b), err)
		}
	}
}
func TestControlTargetAndDuplicate(t *testing.T) {
	dir := fixture(t, "running")
	f, err := os.Create(filepath.Join(dir, "progress.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	r := runner{dir: dir, log: f, s: status{Turn: "new", TurnStatus: "inProgress"}}
	startFixtureWriter(t, &r)
	for i, target := range []string{"old", "new", "new"} {
		id := "key"
		if i == 0 {
			id = "old-key"
		}
		if err := publishMarker(dir, "interrupt", markerOptions{ID: id, Turn: &target}, nil); err != nil {
			t.Fatal(err)
		}
		_, ok, err := r.takeMarker("interrupt")
		if err != nil || ok != (i == 1) {
			t.Fatal(i, ok, err)
		}
	}
}
func TestRPCWriteDeadlineAndLateReply(t *testing.T) {
	reader, writer := io.Pipe()
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	ctx, cancel := fixtureContext(t)
	defer cancel()
	clock := timeshim.NewFake(time.Now())
	s := &server{clock: clock, in: writer, stop: make(chan struct{}), writes: make(chan writeRequest, 1), expired: map[string]bool{"7": true}, writeTimeout: time.Second}
	go s.writer()
	defer close(s.stop)
	done := make(chan error, 1)
	go func() { done <- s.send(fields{"id": 1}) }()
	advanceFixture(t, ctx, clock)
	if err := <-done; !errors.Is(err, errServerClosed) {
		t.Fatal(err)
	}
	if err := s.send(fields{"id": 2}); !errors.Is(err, errServerClosed) {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "progress.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	r := runner{srv: s, log: f, dir: filepath.Dir(f.Name())}
	m, err := r.read(frame{raw: []byte(`{"id":7,"result":{}}`)})
	if err != nil || m.Method == "" || s.expired["7"] {
		t.Fatal(m, err)
	}
}
func TestAtomicSyncOrderAndFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot")
	var order []bool
	syncFile := func(f *os.File) error {
		info, err := f.Stat()
		if err != nil {
			return err
		}
		order = append(order, info.IsDir())
		return f.Sync()
	}
	if err := atomicWriteSync(path, []byte("old"), syncFile); err != nil {
		t.Fatal(err)
	}
	want := []bool{false}
	if runtime.GOOS != "windows" {
		want = append(want, true)
	}
	if !reflect.DeepEqual(order, want) {
		t.Fatal(order)
	}
	if err := atomicWriteSync(path, []byte("new"), func(*os.File) error { return io.ErrClosedPipe }); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil || string(b) != "old" {
		t.Fatal(string(b), err)
	}
}
func TestProgressCursorAndEventSequence(t *testing.T) {
	h := newRuns(t, "complete")
	if err := h.finishFirstTurn("job"); err != nil {
		t.Fatal(err)
	}
	cmd := commands(t)
	t.Setenv("CXRUN_STATE", h.root)
	all := cmd.cli(10, "log", "--job", "job", "--since", "0")
	lines := strings.SplitAfter(all, "\n")
	if len(lines) < 3 {
		t.Fatal(all)
	}
	var prev uint64
	for _, l := range lines[:len(lines)-1] {
		raw, _, _ := strings.Cut(l, "\t")
		seq, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || seq <= prev {
			t.Fatal(l, err)
		}
		prev = seq
	}
	first, _, _ := strings.Cut(lines[0], "\t")
	if got := cmd.cli(10, "log", "--job", "job", "--since", first); got != strings.Join(lines[1:], "") {
		t.Fatal(got)
	}
	b, err := os.ReadFile(filepath.Join(h.root, "job", "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	prev = 0
	for l := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
		var e struct{ Seq uint64 }
		if err = json.Unmarshal([]byte(l), &e); err != nil || e.Seq <= prev {
			t.Fatal(l, err)
		}
		prev = e.Seq
	}
}
func TestRegistryAndReapDeadlines(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := fixtureContext(t)
	defer cancel()
	g, err := registryLock(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	clock := timeshim.NewFake(time.Now())
	done := make(chan error, 1)
	go func() {
		held, lockErr := registryLock(clockContext(ctx, clock), root)
		if held != nil {
			closeBestEffort(held)
		}
		done <- lockErr
	}()
	if waitErr := clock.WaitPending(ctx, 2); waitErr != nil {
		t.Fatal(waitErr)
	}
	clock.Advance(5 * time.Second)
	if err = <-done; !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		t.Fatal(err, ctx.Err())
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, exe, "fixture-command", "silent")
	cmd.Dir = root
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	go func() { done <- waitProcess(cmd, time.Second, clock) }()
	advanceFixture(t, ctx, clock)
	if err = <-done; !errors.Is(err, errReapDeadline) {
		t.Fatal(err)
	}
}
func TestFollowupQueueHandlesInterruptAndIdle(t *testing.T) {
	h := newRuns(t, "manual")
	setTestHost(t, h.root, map[string]int{"agent": 1}, "")
	h.withClock("a")
	a := h.start("a", "--idle-exit", "2s")
	h.await("a", func(s status) bool { return s.Turn == "first" })
	b := h.start("b")
	h.await("b", func(s status) bool { return s.State == "queued" && s.PID != 0 })
	if err := h.ctl("say", "a", "completed"); err != nil {
		t.Fatal(err)
	}
	h.await("b", func(s status) bool { return s.Turn == "first" })
	if err := h.ctl("say", "a", "second round"); err != nil {
		t.Fatal(err)
	}
	h.await("a", func(s status) bool { return s.State == "queued" })
	if err := h.ctl("interrupt", "a"); err != nil {
		t.Fatal(err)
	}
	h.awaitRecord("a", func(row stateRecord) bool {
		var ack inboxAck
		return row.Kind == "inbox_ack" && json.Unmarshal(row.Value, &ack) == nil && ack.Result == "withdrawn"
	})
	if err := h.idleExit("a", a, func(s status) bool { return s.State == "waiting" }); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(h.root, "a", "progress.log"))
	if err != nil || !strings.Contains(string(data), "turn/interrupt\tidle") {
		t.Fatal(string(data), err)
	}
	if err = h.ctl("close", "b"); err != nil {
		t.Fatal(err)
	}
	if err = <-b; err != nil {
		t.Fatal(err)
	}
}

func TestRestartBudgetAndCommandStall(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		count int
	}{{"default", nil, 3}, {"limited", []string{"--max-restarts", "2"}, 2}, {"disabled", []string{"--max-restarts", "0"}, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRuns(t, "crash-loop")
			t.Setenv("GORACE", "atexit_sleep_ms=0")
			if err := <-h.start("a", tc.args...); err == nil {
				t.Fatal("crash loop succeeded")
			}
			s := h.await("a", func(s status) bool { return s.State == "failed" })
			if s.Restarts != tc.count {
				t.Fatal(s)
			}
		})
	}
	h := newRuns(t, "command-silent")
	clock := h.withClock("cmd")
	done := h.start("cmd", "--cmd-stall", "500ms", "--stall", "50ms")
	// The fixture emits item/started before the turn/start reply. Wait for
	// that reply and a subsequent RPC before advancing the command stall clock.
	h.await("cmd", func(s status) bool { return s.Turn == "first" })
	if err := h.ctl("rpc", "cmd", "fixture/noop"); err != nil {
		t.Fatal(err)
	}
	clock.Advance(500 * time.Millisecond)
	h.await("cmd", func(s status) bool { return s.StallCount == 1 })
	if err := h.ctl("close", "cmd"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
func TestStallContinuationRetainsSlot(t *testing.T) {
	root := t.TempDir()
	slot, err := lockPath(filepath.Join(root, "slot"), os.O_CREATE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = slot.Close() }()
	r := runner{slot: slot, dir: root, pending: defaultContinue}
	startFixtureWriter(t, &r)
	if err = r.finish(turn{ID: "t", Status: "interrupted"}); err != nil || r.slot != slot {
		t.Fatal(err, r.slot)
	}
}
func TestRejectedStartReleasesSlot(t *testing.T) {
	root := t.TempDir()
	slot, err := lockPath(filepath.Join(root, "slot"), os.O_CREATE)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = slot.Close() }()
	input, output := io.Pipe()
	defer func() { _ = input.Close() }()
	defer func() { _ = output.Close() }()
	s := &server{in: output, stop: make(chan struct{}), frames: make(chan frame, 1), writes: make(chan writeRequest, 1), writeTimeout: time.Second}
	go s.writer()
	defer close(s.stop)
	go func() { _, _ = io.Copy(io.Discard, input) }()
	s.frames <- frame{raw: []byte(`{"id":1,"error":{"code":-1,"message":"rejected"}}`)}
	ctx, cancel := fixtureContext(t)
	defer cancel()
	r := runner{dir: root, srv: s, slot: slot, ctx: ctx}
	startFixtureWriter(t, &r)
	if err = r.input("hello", "msg", false); err == nil || r.slot != nil {
		t.Fatal(err, r.slot)
	}
}

func TestLateReplyDuringNextCall(t *testing.T) {
	reader, writer := io.Pipe()
	defer func() { _ = reader.Close() }()
	defer func() { _ = writer.Close() }()
	clock := timeshim.NewFake(time.Now())
	s := &server{clock: clock, in: writer, stop: make(chan struct{}), frames: make(chan frame, 2), writes: make(chan writeRequest, 1), expired: map[string]bool{}, writeTimeout: time.Minute, responseTimeout: time.Second}
	go s.writer()
	defer close(s.stop)
	go func() { _, _ = io.Copy(io.Discard, reader) }()
	dir := t.TempDir()
	log, err := os.Create(filepath.Join(dir, "progress.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	ctx, cancel := fixtureContext(t)
	defer cancel()
	r := runner{dir: dir, ctx: ctx, srv: s, log: log}
	startFixtureWriter(t, &r)
	done := make(chan error, 1)
	go func() { done <- r.call("first", nil, nil) }()
	if waitErr := clock.WaitDeadline(ctx, clock.Now().Add(s.responseTimeout)); waitErr != nil {
		t.Fatal(waitErr)
	}
	clock.AdvanceToNext()
	if err = <-done; !errors.Is(err, errRPCUncertain) {
		t.Fatal("RPC did not expire")
	}
	s.frames <- frame{raw: []byte(`{"id":1,"result":{"old":true}}`)}
	s.frames <- frame{raw: []byte(`{"id":2,"result":{"ok":true}}`)}
	var result struct{ OK bool }
	if err = r.call("second", nil, &result); err != nil || !result.OK {
		t.Fatal(result, err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil || !bytes.Contains(data, []byte("rpc/late")) {
		t.Fatal(string(data), err)
	}
}
func TestGoSourcesFormatted(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil || len(paths) == 0 {
		t.Fatal(paths, err)
	}
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		formatted, err := format.Source(b)
		if err != nil || !bytes.Equal(b, formatted) {
			t.Errorf("%s requires gofmt: %v", p, err)
		}
	}
}

func TestPendingControlSurvivesNextPublisher(t *testing.T) {
	dir := fixture(t, "running")
	if err := publishMarker(dir, "interrupt", markerOptions{ID: "first"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := publishMarker(dir, "interrupt", markerOptions{ID: "second"}, nil); err != nil {
		t.Fatal(err)
	}
	r := runner{dir: dir}
	startFixtureWriter(t, &r)
	first, ok, err := r.takeMarker("interrupt")
	if err != nil || !ok || first.ID != "first" {
		t.Fatal(first, ok, err)
	}
	second, ok, err := r.takeMarker("interrupt")
	if err != nil || !ok || second.ID != "second" {
		t.Fatal(second, ok, err)
	}
}
