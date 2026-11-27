package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestAgentToolsAppServerConfig(t *testing.T) {
	ambient, work, _ := runnerFixture(t)
	root := t.TempDir()
	dir := filepath.Join(root, "p", "job")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	r := runner{c: config{Dir: work, Project: "p", JobTools: true, RunID: "run"}, root: root, dir: dir}
	s, err := openServer(r.c, r.root, r.dir)
	if err != nil {
		t.Fatal(err)
	}
	r.srv = s
	t.Cleanup(func() {
		if shutdownErr := r.shutdown(); shutdownErr != nil {
			t.Error(shutdownErr)
		}
	})
	if !slices.Contains(s.cmd.Args, `mcp_servers.cxrun.default_tools_approval_mode="approve"`) {
		t.Error("cxrun MCP tools require an explicit approval mode under approvalPolicy=never")
	}
	for _, arg := range s.cmd.Args {
		raw, ok := strings.CutPrefix(arg, "mcp_servers.cxrun.args=")
		if !ok {
			continue
		}
		var argv []string
		if decodeErr := json.Unmarshal([]byte(raw), &argv); decodeErr != nil ||
			!slices.Equal(argv, []string{"node", "mcp-tools", "--state", root, "--job", "p/job", "--expect-run", "run"}) {
			t.Fatalf("wrong MCP arguments with ambient root %s: %s (%v)", ambient, raw, decodeErr)
		}
		return
	}
	t.Fatal("missing MCP arguments")
}

func TestAgentToolSession(t *testing.T) {
	h := newRuns(t, "manual")
	const id = "p/j"
	clock := h.withClock(id)
	job := h.start(id)
	dir := filepath.Join(h.root, "p", "j")
	h.await(id, func(s status) bool { return s.State == "running" })
	invoke := func(ctx context.Context, name, args string) (string, error) {
		in := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + name + `","arguments":` + args + `}}`)
		var out bytes.Buffer
		err := serveToolsFor(ctx, dir, "", in, &out)
		return out.String(), err
	}
	check := func(name, args, want string) {
		t.Helper()
		s, err := invoke(h.ctx, name, args)
		if err != nil || !strings.Contains(s, want) || !strings.Contains(s, `"isError":false`) {
			t.Fatalf("%s: %v %s", name, err, s)
		}
	}
	check("send", `{"to":"#reports","text":"started"}`, "#reports")
	check("ask", `{"question":"next?"}`, "ask-2")
	baseline := filepath.Join(t.TempDir(), "cursor")
	var wait bytes.Buffer
	if err := waitAny(h.ctx, h.root, []string{"p"}, "", baseline, true, &wait); err != nil || !strings.Contains(wait.String(), `"outbox_seq":2`) {
		t.Fatalf("wait: %s %v", wait.String(), err)
	}
	if err := h.ctl("say", id, "hello"); err != nil {
		t.Fatal(err)
	}
	h.awaitRecord(id, func(row stateRecord) bool {
		var ack inboxAck
		return row.Kind == "inbox_ack" && json.Unmarshal(row.Value, &ack) == nil &&
			ack.Seq == 1 && ack.Result == "delivered"
	})
	check("inbox", "{}", "hello")
	done := make(chan string, 1)
	go func() {
		s, err := invoke(h.ctx, "ask", `{"question":"choose","options":["blue"],"blocking":true,"timeout":120}`)
		if err != nil {
			s = err.Error()
		}
		done <- s
	}()
	h.await(id, func(s status) bool { return s.State == "waiting_input" && s.InputKind == agentAsk })
	fixtureCLI(t, 6, "wait", "--job", id, "--timeout", "2m")
	if err := h.ctl("say", id, "--request", "ask-3", "blue"); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-done:
		if !strings.Contains(s, "blue") {
			t.Fatal(s)
		}
	case <-h.ctx.Done():
		t.Fatal("tool blocked")
	}
	h.await(id, func(s status) bool { return s.State == "running" && len(s.Approvals) == 0 })
	line := fixtureCLI(t, 0, "outbox", "--job", id, "--since", "2")
	var m agentMail
	if err := json.Unmarshal([]byte(line), &m); err != nil || m.Seq != 3 || m.Question != "choose" {
		t.Fatalf("outbox %s %v", line, err)
	}
	type outcome struct {
		err  error
		body string
	}
	expired := make(chan outcome, 1)
	go func() {
		body, err := invoke(clockContext(h.ctx, clock), "ask", `{"question":"deadline","blocking":true,"timeout":120}`)
		expired <- outcome{body: body, err: err}
	}()
	asking := h.await(id, func(s status) bool {
		return s.InputKind == agentAsk && len(s.Approvals) == 1 && s.Approvals[0].Summary == "deadline"
	})
	deadline := asking.Approvals[0].Deadline
	if err := clock.WaitDeadline(h.ctx, deadline); err != nil {
		t.Fatal(err)
	}
	clock.AdvanceTo(deadline)
	select {
	case result := <-expired:
		if result.err != nil || !strings.Contains(result.body, `"isError":true`) {
			t.Fatalf("expired: %s %v", result.body, result.err)
		}
	case <-h.ctx.Done():
		t.Fatal(h.ctx.Err())
	}
	h.awaitRecord(id, func(row stateRecord) bool {
		var cursor stateCursor
		return row.Kind == outboxSeenKind && json.Unmarshal(row.Value, &cursor) == nil && cursor.Seq == 4
	})
	h.await(id, func(s status) bool { return s.State == "running" && len(s.Approvals) == 0 })
	if err := h.ctl("close", id); err != nil {
		t.Fatal(err)
	}
	if err := <-job; err != nil {
		t.Fatal(err)
	}
}

func TestHostFactsFingerprint(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir)
	t.Setenv("HOME", dir)
	t.Setenv("USERPROFILE", dir)
	ctx, cancel := fixtureContext(t)
	defer cancel()
	f := hostFacts(ctx, dir)
	for key := range strings.FieldsSeq("os os_version arch cpu_model cpu_cores memory_total_bytes memory_available_bytes disk_free_bytes load1 cpu_usage_percent engines version_managers") {
		if _, ok := f[key]; !ok {
			t.Fatalf("missing %s", key)
		}
	}
	rows, _ := f["engines"].(fields)
	for _, name := range engineNames() {
		row, ok := rows[name].(fields)
		if !ok || row["path"] != nil || row["version"] != nil || row["login_status"] != nil {
			t.Fatal(f)
		}
	}
	hash := func() string {
		v, err := fingerprint(ctx, dir)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	a := hash()
	if a != hash() {
		t.Fatal("unstable fingerprint")
	}
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example"), 0600); err != nil {
		t.Fatal(err)
	}
	if hash() == a {
		t.Fatal("missed declaration change")
	}
}

func TestMCPHandshake(t *testing.T) {
	in := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	var out bytes.Buffer
	if err := serveToolsFor(context.Background(), t.TempDir(), "", in, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "2025-06-18") {
		t.Fatal(out.String())
	}
	for _, name := range []string{"ask", "send", "inbox"} {
		if !strings.Contains(lines[1], `"name":"`+name+`"`) {
			t.Fatal(lines[1])
		}
	}
}
