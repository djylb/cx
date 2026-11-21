package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

var fixtureRequests = []string{
	"execCommandApproval", "applyPatchApproval", "item/permissions/requestApproval", "item/fileChange/requestApproval", "item/commandExecution/requestApproval",
	"item/tool/requestUserInput", "mcpServer/elicitation/request", "item/tool/call", "account/chatgptAuthTokens/refresh", "attestation/generate",
}

func TestNestedControlRequests(t *testing.T) {
	h := newRuns(t, "silent")
	done := h.start("job", "--idle-exit", "5s")
	h.await("job", func(s status) bool { return s.Turn == "first" })
	call := make(chan error, 1)
	go func() { call <- h.ctl("rpc", "job", "fixture/requests", "--params", "{}") }()
	s := h.await("job", func(s status) bool { return len(s.Approvals) == 7 && s.InputKind == "server_request" })
	for _, a := range s.Approvals {
		if a.Generation != s.Generation || a.Summary == "" || a.Deadline.Before(a.Requested) {
			t.Fatal(a)
		}
	}
	for i := 6; i >= 0; i-- {
		args := []string{"--request", strconv.Itoa(i), "--result", `{"decision":"acceptForSession"}`}
		op := "reply"
		if i == 5 {
			op = "say"
			args = []string{"--request", "5", "answer"}
		}
		if err := h.ctl(op, "job", args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-call; err != nil {
		t.Fatal(err)
	}
	if err := h.ctl("reply", "job", "--request", "0", "--result", "{}"); err == nil {
		t.Fatal("accepted resolved request")
	}
	if err := h.ctl("close", "job"); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestInteractiveAndThreadControls(t *testing.T) {
	h := newRuns(t, "question")
	done := h.start("job", "--interactive", "--idle-exit", "5s")
	h.await("job", func(s status) bool { return s.InputKind == "next_turn" })
	if err := h.ctl("wait", "job"); !errors.Is(err, errQuestion) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(h.ctx, 100*time.Millisecond)
	defer cancel()
	if err := waitAny(ctx, h.root, "", "", "", io.Discard); !errors.Is(err, errObserveTimeout) {
		t.Fatal(err)
	}
	for _, scope := range []string{"turn", "thread"} {
		if err := h.ctl("settings", "job", "--scope", scope, "--effort", "low"); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.ctl("review", "job", "--target", `{"type":"uncommittedChanges"}`, "--delivery", "detached"); err != nil {
		t.Fatal(err)
	}
	h.await("job", func(s status) bool {
		return s.Thread == "thread" && s.ReviewThread == "review" && s.ReviewTurn == "reviewTurn"
	})
	if err := h.ctl("compact", "job", "--op-id", "shrink"); err != nil {
		t.Fatal(err)
	}
	h.await("job", func(s status) bool { return s.CompactOp == "shrink" && s.CompactSeq > s.CompactAfter })
	if err := h.ctl("say", "job", "second round"); err != nil {
		t.Fatal(err)
	}
	h.await("job", func(s status) bool { return s.LastCompleted == "second" && s.State == "waiting" })
	if err := h.ctl("undo", "job", "--before", "first"); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(h.root, "job", "last.md"))
	if err != nil || string(b) != "restored" {
		t.Fatalf("last=%q err=%v", b, err)
	}
	child := make(chan error, 1)
	go func() { child <- h.ctl("fork", "child", "job", "--through", "first") }()
	h.await("child", func(s status) bool { return s.Thread == "child" && s.State == "waiting" })
	for _, id := range []string{"child", "job"} {
		if err := h.ctl("close", id); err != nil {
			t.Fatal(err)
		}
	}
	for _, ch := range []<-chan error{child, done} {
		if err := <-ch; err != nil {
			t.Fatal(err)
		}
	}
}

func TestLostControlReceipt(t *testing.T) {
	h := newRuns(t, "silent")
	done := h.start("job", "--max-restarts", "0")
	h.await("job", func(s status) bool { return s.Turn == "first" })
	for range 2 {
		if err := h.ctl("rpc", "job", "fixture/disconnect", "--op-id", "lost"); err == nil {
			t.Fatal("lost reply reported success")
		}
	}
	if err := <-done; err == nil {
		t.Fatal("fixture unexpectedly survived")
	}
	var receipt rpcReceipt
	if err := readJSON(filepath.Join(h.root, "job", "outbox", "lost.json"), &receipt); err != nil || receipt.State != "uncertain" || receipt.Generation != 1 {
		t.Fatal(receipt, err)
	}
	if _, err := os.Stat(filepath.Join(h.root, "job", "inbox", "uncertain", "lost.rpc")); err != nil {
		t.Fatal(err)
	}
}

func TestLocalHandshake(t *testing.T) {
	if os.Getenv("CXRUN_REAL_SMOKE") != "1" {
		t.Skip("requires local codex app-server")
	}
	dir := t.TempDir()
	t.Setenv("CODEX_HOME", dir)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	r := runner{dir: dir, c: config{Dir: dir}, ctx: ctx}
	for name, ptr := range map[string]**os.File{"events.jsonl": &r.events, "progress.log": &r.log} {
		f, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		*ptr = f
	}
	var err error
	r.srv, err = openServer(r.c, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := r.shutdown(); err != nil {
			t.Error(err)
		}
	}()
	if err := r.initialize(); err != nil {
		t.Fatal(err)
	}
	var result struct{ Data []string }
	if err := r.call("thread/loaded/list", fields{}, &result); err != nil || result.Data == nil {
		t.Fatal(result, err)
	}
}
