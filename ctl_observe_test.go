package main

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/djylb/timeshim"
)

func TestControllerListCachedHost(t *testing.T) {
	f := newControllerFixture(t)
	stateRoot := filepath.Join(f.home, "state")
	for _, s := range []status{
		{Project: "p", Job: "selected", Session: "fixture", State: "done"},
		{Project: "p", Job: "other-session", Session: "different", State: "done"},
		{Project: "q", Job: "other-project", Session: "fixture", State: "done"},
	} {
		dir := filepath.Join(stateRoot, s.Project, s.Job)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := writeFixtureStatus(dir, s); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := fixtureContext(t)
	defer cancel()
	stamp := time.Date(2026, 11, 28, 4, 0, 0, 123, time.UTC)
	clock := timeshim.NewFake(stamp)
	ctx = clockContext(ctx, clock)
	run := func(want ctlExit, args ...string) string {
		t.Helper()
		out, err := os.CreateTemp(t.TempDir(), "stdout")
		if err != nil {
			t.Fatal(err)
		}
		defer closeBestEffort(out)
		prior := os.Stdout
		os.Stdout = out
		defer func() { os.Stdout = prior }()
		err = controller(ctx, append([]string{"ls"}, args...))
		if (want == 0 && err != nil) || (want != 0 && !errors.Is(err, want)) {
			t.Fatalf("ls %v: %v, want exit %d", args, err, want)
		}
		b, err := os.ReadFile(out.Name())
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	live := run(0, "--json", "--project", "p")
	if !strings.Contains(live, "selected") || strings.Contains(live, "other-") || strings.Contains(live, "stale") {
		t.Fatalf("live: %s", live)
	}

	cache := filepath.Join(f.home, ".cache", "cx", "ls", "local.json")
	before, err := os.ReadFile(cache)
	if err != nil {
		t.Fatal(err)
	}

	// Point the local node at a file so its rows request fails.
	config := filepath.Join(f.home, ".config", "cx", "hosts.toml")
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	blocked := filepath.Join(f.home, "blocked-state")
	if err := os.WriteFile(blocked, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, []byte("[hosts.local]\nstate = "+strconv.Quote(blocked)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Hour)
	stale := run(1, "--json", "--all", "--project", "p", "local:p/other-session")
	var row fields
	if err := decodeRows([]byte(stale), &row); err != nil {
		t.Fatalf("cached JSON: %v: %q", err, stale)
	}
	if row["job"] != "other-session" || row["host"] != "local" || row["stale"] != true || row["as_of"] != stamp.Format(time.RFC3339Nano) {
		t.Fatalf("cached JSON: %s", stale)
	}
	table := run(1, "--project", "p")
	if !strings.Contains(table, "selected") || strings.Contains(table, "other-") ||
		!strings.Contains(table, "last successful observation: "+stamp.Format(time.RFC3339Nano)) {
		t.Fatalf("cached table: %s", table)
	}
	after, err := os.ReadFile(cache)
	if err != nil || string(after) != string(before) {
		t.Fatalf("offline listing changed the cache: %v", err)
	}
	if err := os.WriteFile(cache, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if got := run(1, "--json", "--project", "p"); got != "" {
		t.Fatalf("corrupt cache produced rows: %s", got)
	}

	if err := os.Remove(config); err != nil {
		t.Fatal(err)
	}
	recovered := run(0, "--json", "--project", "p")
	if recovered != live {
		t.Fatalf("recovered: %s; initial: %s", recovered, live)
	}
	var refreshed struct {
		AsOf time.Time `json:"as_of"`
	}
	data, err := os.ReadFile(cache)
	if err != nil {
		t.Fatal(err)
	}
	if err := decodeRows(data, &refreshed); err != nil || !refreshed.AsOf.Equal(clock.Now()) {
		t.Fatalf("refreshed snapshot: %s (%v)", data, err)
	}
}

func TestLSSnapshotKeepsNewerSample(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.json")
	newer := time.Date(2026, 11, 28, 4, 0, 0, 0, time.UTC)
	for _, snapshot := range []lsSnapshot{
		{AsOf: newer, Jobs: []fields{{"status": "done"}}},
		{AsOf: newer.Add(-time.Second), Jobs: []fields{{"status": "running"}}},
	} {
		if err := saveLSSnapshot(t.Context(), path, snapshot); err != nil {
			t.Fatal(err)
		}
	}
	got, err := loadLSSnapshot(path)
	if err != nil || !got.AsOf.Equal(newer) || got.Jobs[0]["status"] != "done" {
		t.Fatalf("cached snapshot: %+v (%v)", got, err)
	}
}

func TestListIsolatesApplyLogErrors(t *testing.T) {
	f := newControllerFixture(t)
	for _, s := range []status{
		{Project: "p", Job: "one", RunID: "one", Session: "fixture", State: "done"},
		{Project: "p", Job: "two", RunID: "two", Session: "fixture", State: "done"},
		{Project: "q", Job: "three", RunID: "three", Session: "fixture", State: "done"},
		{Job: "unscoped", RunID: "unscoped", Session: "fixture", State: "done"},
	} {
		if err := writeFixtureStatus(filepath.Join(f.home, "state", s.Project, s.Job), s); err != nil {
			t.Fatal(err)
		}
	}
	for project, log := range map[string]string{
		"p":        "{",
		"q":        `{"host":"local","job":"q/three","run_id":"three","repo":".","turn":2}`,
		"unscoped": "{",
	} {
		dir, err := outputProject(project)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, "apply-log.jsonl"), []byte(log), 0600); err != nil {
			t.Fatal(err)
		}
	}
	out, diag := cliStreams(t, 0, nil, f.cx, "ls", "--all", "--json")
	if strings.Count(diag, "apply log") != 1 || !strings.Contains(diag, "p: apply log") {
		t.Fatalf("diagnostic: %q", diag)
	}
	rows := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(rows) != 4 {
		t.Fatalf("listing: %s", out)
	}
	for _, line := range rows {
		var row fields
		if err := decodeRows([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		_, applied := row["applied_turn"]
		if applied != (row["job"] == "three") {
			t.Fatalf("listing row: %s", line)
		}
	}
}

func TestListWatchBriefRemovalColumns(t *testing.T) {
	f := newControllerFixture(t)
	root := filepath.Join(f.home, "state")
	job := filepath.Join(root, "p", "watched")
	if err := writeFixtureStatus(job, status{Project: "p", Job: "watched", Session: "fixture", State: "done"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(reader)
	prior := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = prior }()
	done := make(chan error, 1)
	go func() {
		err := displayHosts(ctx, endpoints{"local": {State: root}}, "ls", ctlOptions{brief: true, watch: true})
		_ = writer.Close()
		done <- err
	}()
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("watch: %v", err)
		}
	}()
	scanner := bufio.NewScanner(reader)
	if !scanner.Scan() {
		t.Fatalf("missing header: %v", scanner.Err())
	}
	headings := strings.Split(scanner.Text(), "\t")
	if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "p/watched\t") {
		t.Fatalf("missing live row: %q (%v)", scanner.Text(), scanner.Err())
	}
	if err := os.Rename(job, filepath.Join(f.home, "removed")); err != nil {
		t.Fatal(err)
	}
	for scanner.Scan() {
		columns := strings.Split(scanner.Text(), "\t")
		if !slices.Contains(columns, "removed") {
			continue
		}
		if len(columns) != len(headings) || headings[2] != "STATUS" || columns[2] != "removed" {
			t.Fatalf("headings %q, removal %q", headings, columns)
		}
		return
	}
	t.Fatalf("missing removal: %v", scanner.Err())
}
