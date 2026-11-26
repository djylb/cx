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
	"time"
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
	job := h.start("p/j")
	id := "p/j"
	dir := filepath.Join(h.root, "p", "j")
	h.await(id, func(s status) bool { return s.State == "running" })
	invoke := func(name, args string) (string, error) {
		in := strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"` + name + `","arguments":` + args + `}}`)
		var out bytes.Buffer
		err := serveToolsFor(h.ctx, dir, "", in, &out)
		return out.String(), err
	}
	check := func(name, args, want string) {
		t.Helper()
		s, err := invoke(name, args)
		if err != nil || !strings.Contains(s, want) || !strings.Contains(s, `"isError":false`) {
			t.Fatalf("%s: %v %s", name, err, s)
		}
	}
	check("send", `{"to":"#reports","text":"started"}`, "#reports")
	check("ask", `{"question":"next?"}`, "ask-2")
	baseline := filepath.Join(t.TempDir(), "cursor")
	var wait bytes.Buffer
	if err := waitAny(h.ctx, h.root, "p", "", baseline, &wait); err != nil || !strings.Contains(wait.String(), `"outbox_seq":2`) {
		t.Fatalf("wait: %s %v", wait.String(), err)
	}
	if err := h.ctl("say", id, "hello"); err != nil {
		t.Fatal(err)
	}
	h.await(id, func(s status) bool {
		in, err := readInboxState(dir)
		return err == nil && in.acks[1].Result == "delivered"
	})
	check("inbox", "{}", "hello")
	done := make(chan string, 1)
	go func() {
		s, err := invoke("ask", `{"question":"choose","options":["blue"],"blocking":true,"timeout":8}`)
		if err != nil {
			s = err.Error()
		}
		done <- s
	}()
	h.await(id, func(s status) bool { return s.State == "waiting_input" && s.InputKind == agentAsk })
	fixtureCLI(t, 6, "wait", "--job", id, "--timeout", "2s")
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
	expired, err := invoke("ask", `{"question":"deadline","blocking":true,"timeout":0.6}`)
	if err != nil || !strings.Contains(expired, `"isError":true`) {
		t.Fatalf("expired: %s %v", expired, err)
	}
	h.await(id, func(s status) bool {
		in, err := readInboxState(dir)
		return err == nil && in.outbox.Seq == 4 && s.State == "running" && len(s.Approvals) == 0
	})
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
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	f := hostFacts(ctx, dir)
	for _, key := range strings.Fields("os os_version arch cpu_model cpu_cores memory_total_bytes memory_available_bytes disk_free_bytes load1 cpu_usage_percent engines version_managers") {
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
