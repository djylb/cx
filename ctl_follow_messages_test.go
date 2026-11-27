package main

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTailMessages(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("CLAUDE_CODE_HOST_SESSION_ID", "local_x")
	const job = "p/messages"
	dir := filepath.Join(os.Getenv("CXRUN_STATE"), filepath.FromSlash(job))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := writeFixtureStatus(dir, status{State: "done", Finished: 2}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "progress.log"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "events.jsonl")
	log, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(log)
	var offset int64
	appendEvent := func(method string, params fields, meta fields) int64 {
		t.Helper()
		seq := offset + 1
		record := fields{"seq": seq, "method": method, "params": params}
		maps.Copy(record, meta)
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		n, err := log.Write(append(data, '\n'))
		if err != nil {
			t.Fatal(err)
		}
		offset += int64(n)
		return seq
	}
	message := func(text string) fields { return fields{"item": fields{"type": "agentMessage", "text": text}} }
	const first = "First message"
	long := "## Summary\n" + strings.Repeat("甲", 2048) + "\n## Details\nbody\n## Next steps\nlast line\n"
	seq := appendEvent("item/completed", message(first), nil)
	appendEvent("item/agentMessage/delta", fields{"delta": first}, nil)
	appendEvent("turn/completed", fields{"turn": fields{"id": "one", "items": []fields{message(first)}}}, nil)
	appendEvent("item/completed", fields{"item": fields{"type": "commandExecution", "text": "ignored"}}, nil)
	const stamp = "2026-11-22T00:00:00Z"
	appendEvent("item/completed", message(long), fields{"turn_number": 2, "time": stamp})
	second := "--- turn 2, " + stamp + "\n" + long
	all := "--- turn 1, unknown\n" + first + "\n" + second
	base := []string{"tail", job, "--messages", "--cursor", "review", "--timeout", "2m"}
	check := func(want string, extra ...string) {
		t.Helper()
		out := cliOutput(t, 0, nil, f.cx, append(base, extra...)...)
		if string(out) != want {
			t.Fatalf("%v: got %d bytes, want %d; matching content: %t", extra, len(out), len(want), string(out) == want)
		}
	}
	check(all)
	check("")
	appendEvent("item/completed", message("another"), fields{"turn_number": 2, "time": stamp})
	last := "--- turn 2, " + stamp + "\nanother\n"
	check(last)
	check(second+last, "--since", fmt.Sprint(seq))
	check(all+last, "--from-start")
	cliOutput(t, 2, nil, f.cx, "tail", job, "--messages", "--raw")
}
