package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestModuleClaims(t *testing.T) {
	req := moduleWork{ModuleRepository: "app", Definitions: []moduleDefinition{
		{Name: "ui", Paths: []string{"src/ui/**"}, Deps: []string{"shared"}},
	}}
	for _, tc := range []struct {
		name, repo          string
		claim               moduleClaim
		overlap, dependency bool
	}{
		{"same name", "lib", moduleClaim{Name: "ui"}, true, false},
		{"path overlap", "app", moduleClaim{Name: "old-ui", Paths: []string{"src/ui/panel"}}, true, false},
		{"sibling prefix", "app", moduleClaim{Name: "api", Paths: []string{"src/ui-api"}}, false, false},
		{"separate repository", "lib", moduleClaim{Name: "api", Paths: []string{"src/ui/panel"}}, false, false},
		{"overlapping dependency", "app", moduleClaim{Name: "shared", Paths: []string{"src/ui/panel"}}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			overlap, dependency := moduleClaimRelation(req, tc.repo, []moduleClaim{tc.claim})
			if (len(overlap) > 0) != tc.overlap || (len(dependency) > 0) != tc.dependency {
				t.Fatalf("overlap %v, dependency %v", overlap, dependency)
			}
		})
	}
	files := []string{"internal/server/main.go", "foo/bar/generated_gen.go", "generated_gen.go", "foo/my.go", "internal/serverless/main.go"}
	want := []string{"foo/bar/generated_gen.go", "generated_gen.go", "internal/server/main.go"}
	if got := hotspotFiles(files, []string{"**/*_gen.go", "internal/server/**"}); !slices.Equal(got, want) {
		t.Fatalf("hotspots = %v, want %v", got, want)
	}
}

func TestModuleConflictAndOverrideCLI(t *testing.T) {
	f := newControllerFixture(t)
	snapWriteTest(t, filepath.Join(f.home, ".config", "cx", "hosts.toml"), "[hosts.worker]\nssh = \"worker\"\nplatform = \"linux/amd64\"\n")
	ssh := `#!/bin/sh
set -eu
while test "$1" != --; do shift; done
test "$2" = worker
IFS= read -r request || :
case "$request" in
*'"rows","p",""'*) printf '%s\n' '[{"project":"p","job":"remote","status":"running","module_claims":[{"name":"ui/sub"}]}]' ;;
*) exit 99 ;;
esac
`
	if err := os.WriteFile(filepath.Join(f.home, "ssh"), []byte(ssh), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	src := filepath.Join(f.home, "input")
	snapWriteTest(t, filepath.Join(src, "ui", "file"), "fixture")
	const ownerID = "p/active"
	dir := filepath.Join(f.home, "state", "p", "active")
	if err := writeFixtureStatus(dir, status{Project: "p", Job: "active", RunID: "fixture", State: "waiting", Session: "another"}); err != nil {
		t.Fatal(err)
	}
	owner, err := lockPath(filepath.Join(dir, "owner.lock"), os.O_CREATE)
	if err != nil || owner == nil {
		t.Fatalf("fixture owner: %v", err)
	}
	t.Cleanup(func() { closeBestEffort(owner) })
	work := filepath.Join(f.home, "state", ".work", "p", "active")
	req := jobSubmission{ID: ownerID, moduleWork: moduleWork{
		Modules: []string{"ui"}, Definitions: []moduleDefinition{{Name: "ui", Paths: []string{"ui"}}},
	}}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	snapWriteTest(t, filepath.Join(work, "request.json"), string(data))
	_, blocked := cliStreams(t, 15, nil, f.cx, "submit", "local", src, "declined", "--module", "ui/sub", "--cmd", "--", "sh", "-c", "exit 0")
	if !strings.Contains(blocked, "STATUS: BLOCKED") || !strings.Contains(blocked, "local:"+ownerID) || !strings.Contains(blocked, "worker:p/remote") {
		t.Fatalf("conflict stderr = %s", blocked)
	}
	const reason = "coordinated handoff"
	cliOutput(t, 0, nil, f.cx, "run", "local", src, "allowed", "--module", "ui/sub", "--module-override", reason, "--sync", "--cmd", "--", "sh", "-c", "exit 0")
	cliOutput(t, 0, nil, f.cx, "wait", "p/allowed", "--timeout", "10s")
	var queued queuedJob
	submission, err := submissionDir("local", "p/allowed")
	if err != nil {
		t.Fatal(err)
	}
	if err = readJSON(filepath.Join(submission, "input.json"), &queued); err != nil || queued.Request.ModuleOverride != reason {
		t.Fatalf("queued override = %q, %v", queued.Request.ModuleOverride, err)
	}
	log, err := os.ReadFile(filepath.Join(submission, "submit.log"))
	if err != nil || !strings.Contains(string(log), reason) {
		t.Fatalf("submit log = %s, %v", log, err)
	}
	if got := cliOutput(t, 0, nil, f.cx, "ls", "--all", "--module", "ui/sub", "--brief"); !strings.Contains(string(got), "MODULE") || !strings.Contains(string(got), "p/allowed") || strings.Contains(string(got), ownerID) {
		t.Fatalf("module listing = %s", got)
	}
	cliOutput(t, 0, nil, f.cx, "close", "p/allowed")
}

func checkModuleFixturePrompt(text string) error {
	for _, line := range []string{
		"Declared modules: core, docs.",
		"Direct dependencies of core: docs.",
		"$CX_CONTEXT/modules/",
		"Context package for docs failed to generate.",
		"Changeset: release-7",
	} {
		if !strings.Contains(text, line) {
			return fmt.Errorf("prompt omitted %q", line)
		}
	}
	original, err := os.ReadFile("hotspot.txt")
	if err != nil || string(original) != "base\n" {
		return fmt.Errorf("context command changed the checkout: %q (%v)", original, err)
	}
	if os.Getenv("CX_CHANGESET") != "release-7" {
		return errors.New("agent did not receive CX_CHANGESET")
	}
	files, err := filepath.Glob(filepath.Join(os.Getenv("CX_CONTEXT"), "modules", "*", "core.md"))
	if err != nil || len(files) != 1 {
		return fmt.Errorf("expected one context publication: %v, %v", files, err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil || string(data) != "pinned context\n" {
		return fmt.Errorf("generated context = %q (%v)", data, err)
	}
	return nil
}

func TestModuleContextAcceptanceChangesetCLI(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(f.home, "config"))
	t.Setenv("FIXMODE", "module-work")
	if err := os.Symlink(f.cx, filepath.Join(f.home, "codex")); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(f.home, "repo")
	snapWriteTest(t, filepath.Join(src, "hotspot.txt"), "base\n")
	const table = `{"modules":[
		{"name":"core","paths":["src/**"],"deps":["docs"],"context":["sh","-c","printf 'modified by context' > hotspot.txt; printf 'pinned context\\n'"],"accept":[["sh","-c","printf 'module passed'"]],"hotspot":true},
		{"name":"docs","paths":["docs/**"],"context":["sh","-c","echo 'context failed intentionally' >&2; exit 7"],"accept":[["sh","-c","echo 'module failed'; exit 3"]]}],
		"hotspots":["**/hotspot.txt"]}`
	snapWriteTest(t, filepath.Join(src, "modules.json"), table)
	cliOutput(t, 0, nil, "git", "-C", src, "init", "-q")
	cliOutput(t, 0, nil, "git", "-C", src, "add", ".")
	cliOutput(t, 0, nil, "git", "-C", src, "-c", "user.name=fixture", "-c", "user.email=fixture@invalid", "commit", "-qm", "baseline")
	head := strings.TrimSpace(string(cliOutput(t, 0, nil, "git", "-C", src, "rev-parse", "HEAD")))
	t.Setenv("CX_FIXTURE_PRIVATE_TOKEN", "fixture-private")
	cfg := fmt.Sprintf("modules_cmd = [\"sh\", \"-c\", %q]\nmodules_repo = \"repo\"\n[[repositories]]\npath = %q\n",
		`test -z "${CX_FIXTURE_PRIVATE_TOKEN+x}" && cat modules.json`, src)
	snapWriteTest(t, filepath.Join(f.home, "config", "cx", "projects", "p.toml"), cfg)
	snapshot := strings.TrimSpace(string(cliOutput(t, 0, nil, f.cx, "snapshot", src)))
	snapWriteTest(t, filepath.Join(src, "modules.json"), "{}")
	prompt := filepath.Join(f.home, "prompt.txt")
	snapWriteTest(t, prompt, "task")
	_, diagnostic := cliStreams(t, 0, nil, f.cx, "run", "local", src, "work", prompt, "--snapshot", snapshot, "--sync", "--context", "none",
		"--module", "core", "--module", "docs", "--changeset", "release-7", "--accept-timeout", "5s", "--no-tools",
		"--accept", `test "$CX_CHANGESET" = release-7 && printf 'acceptance edit' > hotspot.txt && printf 'explicit passed'`)
	if !strings.Contains(diagnostic, "serialized") {
		t.Fatalf("missing hotspot warning: %s", diagnostic)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			cliOutput(t, 0, nil, f.cx, "close", "p/work")
		}
	})
	cliOutput(t, 0, nil, f.cx, "result", "p/work", "--wait", "--timeout", "15s", "--json")
	ctx, cancel := fixtureContext(t)
	defer cancel()
	if err := awaitFixtureCondition(ctx, func() (bool, error) {
		receipt := filepath.Join(f.home, "state", ".work", "p", "work", "result", "turn-1", acceptanceFilename)
		_, err := os.Stat(receipt)
		if os.IsNotExist(err) {
			return false, nil
		}
		return err == nil, err
	}); err != nil {
		t.Fatal(err)
	}
	collected := cliOutput(t, 0, nil, f.cx, "result", "p/work", "--turn", "1", "--json")
	var result resultOutput
	if err := json.Unmarshal(collected, &result); err != nil {
		t.Fatal(err)
	}
	if result.AcceptanceStatus != "failed" || result.Changeset != "release-7" || result.ReadsPath == nil || result.Status == nil || *result.Status != "waiting" || len(result.Acceptance) != 3 {
		t.Fatalf("result = %s", collected)
	}
	for i, want := range []struct {
		module, text string
		exit         int64
	}{
		{"core", "module passed", 0}, {"docs", "module failed", 3}, {"", "explicit passed", 0},
	} {
		got := result.Acceptance[i]
		if got.Module != want.module || got.Exit != want.exit || !strings.Contains(got.Tail, want.text) || len(got.Cmd) == 0 {
			t.Fatalf("check %d = %+v", i, got)
		}
	}
	if summary := string(cliOutput(t, 0, nil, f.cx, "result", "p/work")); !strings.Contains(summary, "Acceptance PASS") || !strings.Contains(summary, "Acceptance FAIL") {
		t.Fatalf("summary = %s", summary)
	}
	log, err := os.ReadFile(filepath.Join(f.home, "state", "p", "work", "progress.log"))
	if err != nil || !strings.Contains(string(log), "docs exit=7") || !strings.Contains(string(log), "context failed intentionally") {
		t.Fatalf("context log = %s (%v)", log, err)
	}
	for _, data := range [][]byte{
		cliOutput(t, 0, nil, f.cx, "result", "p/work", "--reads"),
		cliOutput(t, 0, nil, f.cx, "result", "p/work", "--turn", "1", "--reads"),
		cliOutput(t, 0, nil, f.cx, "result", "p/work", "--reads", "--all"),
	} {
		var reads turnReads
		if err := json.Unmarshal(data, &reads); err != nil || reads.Turn != 1 || !slices.Equal(reads.Paths, []readObservation{{"modules.json", "structured"}, {"notes with spaces.md", "parsed"}}) {
			t.Fatalf("reads = %s (%v)", data, err)
		}
	}
	if output := string(cliOutput(t, 0, nil, f.cx, "apply", "p/work")); !strings.Contains(output, "Hotspot: repo/hotspot.txt") {
		t.Fatalf("apply output = %s", output)
	}
	if got := strings.TrimSpace(string(cliOutput(t, 0, nil, "git", "-C", src, "rev-parse", "HEAD"))); got != head {
		t.Fatalf("apply changed HEAD: %s != %s", got, head)
	}
	if content, err := os.ReadFile(filepath.Join(src, "hotspot.txt")); err != nil || string(content) != "updated\n" {
		t.Fatalf("acceptance changed the delivered tree: %q (%v)", content, err)
	}
	cliOutput(t, 0, nil, f.cx, "close", "p/work")
	closed = true
	cliOutput(t, 0, nil, f.cx, "clean", "p/work")
	var listing struct {
		Changeset    string
		Role         string
		Applications []appliedRepo
	}
	b := cliOutput(t, 0, nil, f.cx, "ls", "--all", "--changeset", "release-7", "--module", "core", "--json")
	if err := json.Unmarshal(b, &listing); err != nil || listing.Changeset != "release-7" || listing.Role != "worker" || len(listing.Applications) != 1 ||
		listing.Applications[0].Repo != "repo" || listing.Applications[0].Turn != 1 || listing.Applications[0].TargetHead == nil ||
		*listing.Applications[0].TargetHead != head || !snapOID(listing.Applications[0].ResultTree) ||
		listing.Applications[0].TargetRepositoryKey == "" || listing.Applications[0].Operation == "" {
		t.Fatalf("changeset listing = %s (%v)", b, err)
	}
	if got := cliOutput(t, 0, nil, f.cx, "ls", "--all", "--changeset", "wrong", "--json"); len(got) != 0 {
		t.Fatalf("changeset filter included %s", got)
	}
}
