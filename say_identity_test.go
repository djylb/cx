package main

import (
	"context"
	"crypto/sha256"
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

func TestControllerSayValidatesBeforeResume(t *testing.T) {
	f := newControllerFixture(t)
	dir := filepath.Join(os.Getenv("CXRUN_STATE"), "p", "ended")
	if err := writeFixtureStatus(dir, status{Project: "p", Job: "ended", RunID: "fixture", State: "done", Engine: "codex"}); err != nil {
		t.Fatal(err)
	}
	_, diagnostic := cliStreams(t, 1, nil, f.cx, "say", "local:p/ended")
	if !strings.Contains(diagnostic, "say requires text or @file") {
		t.Fatalf("unexpected diagnostic: %s", diagnostic)
	}
}

func TestControllerSayLongTextOverSSH(t *testing.T) {
	f := newControllerFixture(t)
	dir := filepath.Join(os.Getenv("CXRUN_STATE"), "p", "long")
	if err := writeFixtureStatus(dir, status{Project: "p", Job: "long", RunID: "fixture", State: "running", Engine: "codex", TurnStatus: "inProgress"}); err != nil {
		t.Fatal(err)
	}
	owner, err := lockPath(filepath.Join(dir, "owner.lock"), 0)
	if err != nil || owner == nil {
		t.Fatalf("lock owner: %v", err)
	}
	defer closeBestEffort(owner)

	setupPrefetchSSH(t, f, "")
	// Model a remote argv limit while running the real node wire decoder.
	script := `#!/bin/sh
for arg do line=$arg; done
if [ "${#line}" -gt 32768 ]; then
  echo 'remote argument list too long' >&2
  exit 255
fi
exec sh -c "$line"
`
	if err = os.WriteFile(filepath.Join(f.home, "ssh"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	message := strings.Repeat("- 世界 '$' \n", 6000)
	file := filepath.Join(f.home, "message.txt")
	if err = os.WriteFile(file, []byte(message), 0600); err != nil {
		t.Fatal(err)
	}
	const job = "remote:p/long"
	cliOutput(t, 0, nil, f.cx, "say", job, "--", message)
	attachment := filepath.Join(dir, "attachments", fmt.Sprintf("%x.txt", sha256.Sum256([]byte(message))))
	if text, readErr := os.ReadFile(attachment); readErr != nil || string(text) != message {
		t.Fatalf("inline text did not use the file delivery path: %v", readErr)
	}
	cliOutput(t, 0, nil, f.cx, "say", job, "@"+file)
	messages, err := pendingInboxMessages(dir)
	if err != nil || len(messages) != 2 {
		t.Fatalf("expected two messages, got %d: %v", len(messages), err)
	}
	for _, record := range messages {
		var msg inboxMessage
		if err = json.Unmarshal(record.Value, &msg); err != nil || msg.Text != message {
			t.Fatalf("incorrect text delivery: %d bytes, %v", len(msg.Text), err)
		}
	}
	_, diag := cliStreams(t, 2, nil, f.cx, "say", job, "@"+file+".missing")
	if !strings.Contains(diag, file+".missing") {
		t.Fatalf("missing operand diagnostic: %s", diag)
	}
	cliOutput(t, 11, nil, f.cx, "say", "remote:p/missing", "message")
	if err = os.WriteFile(filepath.Join(f.home, "ssh"), []byte("#!/bin/sh\necho 'remote argument list too long' >&2\nexit 255\n"), 0700); err != nil {
		t.Fatal(err)
	}
	_, diag = cliStreams(t, 9, nil, f.cx, "say", job, "message")
	if !strings.Contains(diag, "remote argument list too long") {
		t.Fatalf("missing transport diagnostic: %s", diag)
	}
}

func TestControllerBriefCompletedJobs(t *testing.T) {
	for _, name := range []string{"no-thread", "command", "reclaimed", "missing-source"} {
		t.Run(name, func(t *testing.T) {
			f := newControllerFixture(t)
			root := os.Getenv("CXRUN_STATE")
			dir := filepath.Join(root, "p", name)
			s := status{Project: "p", Job: name, Session: "fixture", RunID: "fixture", State: "done", Engine: "codex"}
			switch name {
			case "command":
				s.Engine = "cmd"
			case "missing-source":
				s.Thread = "saved"
				source := filepath.Join(root, ".work", "p", "source", "work")
				if err := register(filepath.Dir(dir), dir, config{Dir: source}, s); err != nil {
					t.Fatal(err)
				}
			case "reclaimed":
				s.Thread = "saved"
				work := filepath.Join(root, ".work", "p", name)
				if err := os.MkdirAll(work, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(work, "request.json"), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(filepath.Join(work, "input"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(work, "input", "manifest.json"), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
				source := filepath.Join(work, "work")
				if err := os.Mkdir(source, 0700); err != nil {
					t.Fatal(err)
				}
				if err := publishPreparedSnapshot(work, PreparedSnapshot{Source: source}); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(source); err != nil {
					t.Fatal(err)
				}
			}
			if err := writeFixtureStatus(dir, s); err != nil {
				t.Fatal(err)
			}
			reason := map[string]string{"no-thread": "no saved thread", "command": "command jobs cannot be replayed", "reclaimed": "checkout was reclaimed", "missing-source": "no such file or directory"}[name]
			_, diagnostic := cliStreams(t, 1, nil, f.cx, "say", "local:p/"+name, "continue")
			if !strings.Contains(diagnostic, reason) {
				t.Fatalf("unexpected diagnostic: %s", diagnostic)
			}
			for _, args := range [][]string{{"ls", "--brief", "--json"}, {"node", "list", "--fields", "brief"}} {
				out := cliOutput(t, 0, nil, f.cx, args...)
				var row struct{ Status, Phase string }
				if err := json.Unmarshal(out, &row); err != nil || row.Status != "done" || row.Phase != "completed" {
					t.Fatalf("unexpected listing: %s (%v)", out, err)
				}
			}
		})
	}
}

type saySealClock struct {
	*timeshim.Fake
	onSealWait func()
}

func (c saySealClock) AfterFunc(d time.Duration, f func()) timeshim.Timer {
	timer := c.Fake.AfterFunc(d, f)
	if d == 20*time.Millisecond {
		c.onSealWait()
		go c.Advance(d)
	}
	return timer
}

func TestSayRejectsReplacementDuringSeal(t *testing.T) {
	for _, operands := range [][]string{{"--cx-text", "message"}, {"--input-json=[{\"type\":\"text\",\"text\":\"message\"}]"}} {
		t.Run(operands[0], func(t *testing.T) {
			newControllerFixture(t)
			root := os.Getenv("CXRUN_STATE")
			dir := filepath.Join(root, "p", "job")
			s := status{Project: "p", Job: "job", State: "waiting", Engine: "codex", RunID: "fixture", Thread: "saved", Finished: 1, LastCompleted: "turn"}
			err := writeFixtureStatus(dir, s)
			if err != nil {
				t.Fatal(err)
			}
			owner, lockErr := lockPath(filepath.Join(dir, "owner.lock"), 0)
			if lockErr != nil || owner == nil {
				t.Fatalf("lock owner: %v", lockErr)
			}
			defer closeBestEffort(owner)
			result := filepath.Join(dir, "result")
			if err = os.MkdirAll(result, 0700); err != nil {
				t.Fatal(err)
			}
			seal, lockErr := lockPath(filepath.Join(result, ".lock"), os.O_CREATE)
			if lockErr != nil || seal == nil {
				t.Fatalf("lock result: %v", lockErr)
			}
			defer closeBestEffort(seal)
			replaced := false
			clock := saySealClock{Fake: timeshim.NewFake(time.Unix(100, 0)), onSealWait: func() {
				if replaced {
					t.Fatal("unexpected second seal wait")
				}
				replaced = true
				if renameErr := os.Rename(dir, dir+"-old"); renameErr != nil {
					t.Fatal(renameErr)
				}
				if writeErr := writeFixtureStatus(dir, s); writeErr != nil {
					t.Fatal(writeErr)
				}
				folder := filepath.Join(result, "turn-1")
				if mkdirErr := os.MkdirAll(folder, 0700); mkdirErr != nil {
					t.Fatal(mkdirErr)
				}
				if writeErr := os.WriteFile(filepath.Join(folder, "result.json"), []byte("{}"), 0600); writeErr != nil {
					t.Fatal(writeErr)
				}
			}}
			ctx := context.WithValue(t.Context(), clockKey{}, clock)
			err = nodeAction(ctx, append([]string{"say", "p/job", "local"}, operands...))
			if !replaced || !errors.Is(err, errJobReplaced) {
				t.Fatalf("replaced=%v, say error=%v", replaced, err)
			}
			messages, err := pendingInboxMessages(dir)
			if err != nil || len(messages) != 0 {
				t.Fatalf("replacement inbox: %v, %v", messages, err)
			}
		})
	}
}
