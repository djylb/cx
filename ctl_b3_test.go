package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestB3ControllerContract(t *testing.T) {
	f := newControllerFixture(t)
	programs := []string{f.cx}
	if f.python == "" {
		t.Log("cx-control.py unavailable: skipping project and controller output comparisons")
	} else {
		programs = append(programs, "python3")
	}
	repo := filepath.Join(f.home, "repo")
	cliOutput(t, 0, nil, "git", "init", "--quiet", repo)
	var previous any
	for _, program := range programs {
		t.Setenv("XDG_CONFIG_HOME", filepath.Join(f.home, filepath.Base(program)+"-config"))
		invoke := func(code int, args ...string) []byte {
			if program == "python3" {
				args = append([]string{f.python}, args...)
			}
			return cliOutput(t, code, nil, program, args...)
		}
		if got := invoke(0, "project", "init", "demo", "--default-host", "local", "--repo", repo, repo); string(got) != "demo\n" {
			t.Fatal(string(got))
		}
		invoke(2, "project", "init", "demo", "--repo", repo)
		if got := invoke(0, "project", "ls"); string(got) != "demo\n" {
			t.Fatal(string(got))
		}
		var def any
		if e := json.Unmarshal(invoke(0, "project", "show", "demo"), &def); e != nil {
			t.Fatal(e)
		}
		if previous != nil && !reflect.DeepEqual(previous, def) {
			t.Fatalf("different definitions: %v / %v", previous, def)
		}
		previous = def
	}
	script := filepath.Join(f.home, "long script")
	if e := os.WriteFile(script, append(append([]byte("#"), bytes.Repeat([]byte("x"), 12000)...), []byte("\nprintf '%s\\n' 'literal $HOME'\nexit 17\n")...), 0600); e != nil {
		t.Fatal(e)
	}
	if got := cliOutput(t, 17, nil, f.cx, "exec", "local", "--tag", "script", "--wait", "--script", script, "--shell", "sh"); string(got) != "literal $HOME\n" {
		t.Fatal(string(got))
	}
	for _, op := range []string{"requests", "plan", "diff", "events", "outbox"} {
		args := []string{op, "local:p/script"}
		goOut := cliOutput(t, 0, nil, f.cx, args...)
		if f.python == "" {
			continue
		}
		pyOut := cliOutput(t, 0, nil, "python3", append([]string{f.python}, args...)...)
		if !bytes.Equal(goOut, pyOut) {
			var a, b any
			if json.Unmarshal(goOut, &a) != nil || json.Unmarshal(pyOut, &b) != nil || !reflect.DeepEqual(a, b) {
				t.Fatalf("%s: %q / %q", op, goOut, pyOut)
			}
		}
	}
}

func TestB3SnapshotResult(t *testing.T) {
	f := newControllerFixture(t)
	src := filepath.Join(f.home, "input")
	if e := os.Mkdir(src, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(src, "original"), []byte("base\n"), 0600); e != nil {
		t.Fatal(e)
	}
	cliOutput(t, 0, nil, f.cx, "run", "local", src, "roundtrip", "--cmd", "--", "sh", "-c", "echo changed > original && mkdir artifacts && echo asset > artifacts/test && echo report")
	cliOutput(t, 0, nil, f.cx, "wait", "p/roundtrip", "--timeout", "3s")
	cliOutput(t, 12, nil, f.cx, "result", "p/roundtrip")
	if got := cliOutput(t, 0, nil, f.cx, "result", "p/roundtrip", "--wait"); !bytes.Contains(got, []byte("artifacts/test (6 bytes)")) {
		t.Fatal(string(got))
	}
	if got := cliOutput(t, 0, nil, f.cx, "patch", "p/roundtrip", "--repo", "input", "--full"); !bytes.Contains(got, []byte("+changed\n")) {
		t.Fatal(string(got))
	}
}

func TestB3CenterBuildAndTaskResume(t *testing.T) {
	f := newControllerFixture(t)
	module := filepath.Join(f.home, "module")
	if e := os.Mkdir(module, 0700); e != nil {
		t.Fatal(e)
	}
	for n, s := range map[string]string{"go.mod": "module cx\n\ngo 1.23\n", "go.sum": "", "ctl_deploy.go": "package main\nimport _ \"embed\"\n//go:embed message.txt\nvar message string\nfunc main(){print(message)}\n", "message.txt": "fixture"} {
		if e := os.WriteFile(filepath.Join(module, n), []byte(s), 0600); e != nil {
			t.Fatal(e)
		}
	}
	t.Setenv("CX_MODULE", module)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	id, m, e := freezeCompilerSource(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if len(m.Repositories) != 1 {
		t.Fatal(m)
	}
	binary, e := compileSnapshot(ctx, string(id), runtime.GOOS+"/"+runtime.GOARCH)
	if e != nil {
		t.Fatal(e)
	}
	if st, statErr := os.Stat(binary); statErr != nil || st.Size() == 0 {
		t.Fatal(statErr)
	}
	first, e := os.Stat(binary)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = compileSnapshot(ctx, string(id), runtime.GOOS+"/"+runtime.GOARCH); e != nil {
		t.Fatal(e)
	}
	again, e := os.Stat(binary)
	if e != nil || !os.SameFile(first, again) {
		t.Fatal("cache miss", e)
	}
	req := jobSubmission{ID: "p/packet", Host: "local", Source: module, Prompt: "abc"}
	raw, e := json.Marshal(req)
	if e != nil {
		t.Fatal(e)
	}
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	if _, err := z.Write(raw[:15]); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	args := []string{"node", "task-receive", req.ID, snapSum(raw), strconv.Itoa(len(raw)), "2s"}
	cliOutput(t, 1, compressed.Bytes(), f.cx, args...)
	compressed.Reset()
	z.Reset(&compressed)
	if _, err := z.Write(raw[15:]); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	out := cliOutput(t, 0, compressed.Bytes(), f.cx, args...)
	if !bytes.HasPrefix(out, []byte("15\n")) {
		t.Fatal(string(out))
	}
	if got := cliOutput(t, 0, nil, f.cx, args...); string(got) != "done\n" {
		t.Fatal(string(got))
	}
}

func TestB3AllRepositoryPreflight(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("GIT_AUTHOR_NAME", "fixture")
	t.Setenv("GIT_COMMITTER_NAME", "fixture")
	t.Setenv("GIT_AUTHOR_EMAIL", "fixture@example.invalid")
	t.Setenv("GIT_COMMITTER_EMAIL", "fixture@example.invalid")
	root := filepath.Join(f.home, "module-set")
	src := filepath.Join(root, "first")
	other := filepath.Join(root, "second")
	for _, repo := range []string{src, other} {
		cliOutput(t, 0, nil, "git", "init", "--quiet", repo)
		for k, v := range map[string]string{"a": "base\n", "go.mod": "module " + filepath.Base(repo) + "\n\ngo 1.23\n"} {
			if e := os.WriteFile(filepath.Join(repo, k), []byte(v), 0600); e != nil {
				t.Fatal(e)
			}
		}
		cliOutput(t, 0, nil, "git", "-C", repo, "add", ".")
		cliOutput(t, 0, nil, "git", "-C", repo, "commit", "-qm", "fixture")
	}
	work := filepath.Join(root, "go.work")
	if e := os.WriteFile(work, []byte("go 1.23\nuse (\n ./first\n ./second\n)\n"), 0600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("GOWORK", work)
	cliOutput(t, 0, nil, f.cx, "run", "local", src, "merge", "--cmd", "--", "sh", "-c", "echo worker > a; echo worker > ../second/a")
	cliOutput(t, 0, nil, f.cx, "wait", "p/merge", "--timeout", "4s")
	if e := os.WriteFile(filepath.Join(other, "a"), []byte("local\n"), 0600); e != nil {
		t.Fatal(e)
	}
	index := filepath.Join(src, ".git", "index")
	before, e := os.ReadFile(index)
	if e != nil {
		t.Fatal(e)
	}
	cliOutput(t, 1, nil, f.cx, "apply", "p/merge")
	b, e := os.ReadFile(filepath.Join(src, "a"))
	if e != nil || string(b) != "base\n" {
		t.Fatal(string(b), e)
	}
	after, e := os.ReadFile(index)
	if e != nil || !bytes.Equal(before, after) {
		t.Fatal("preflight changed first index", e)
	}
	if e = os.WriteFile(filepath.Join(other, "a"), []byte("base\n"), 0600); e != nil {
		t.Fatal(e)
	}
	cliOutput(t, 0, nil, f.cx, "apply", "p/merge")
	for _, repo := range []string{src, other} {
		if b, e = os.ReadFile(filepath.Join(repo, "a")); e != nil || string(b) != "worker\n" {
			t.Fatal(string(b), e)
		}
	}
	log, e := os.ReadFile(filepath.Join(f.home, "out", "p", "apply-log.jsonl"))
	if e != nil || bytes.Count(log, []byte("\n")) != 2 {
		t.Fatal(string(log), e)
	}
}

func TestB3InteractiveContinuation(t *testing.T) {
	f := newControllerFixture(t)
	_, src, _ := runnerFixture(t)
	t.Setenv("FIXMODE", "question")
	ctxDir := filepath.Join(f.home, "context")
	if e := os.Mkdir(ctxDir, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(ctxDir, "interactive.md"), []byte("Workflow"), 0600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("CX_CONTEXT_DIR", ctxDir)
	t.Setenv("FIXPROMPT", "Workflow\nProject context is available in $CX_CONTEXT.\n\ntask")
	out := cliOutput(t, 6, nil, f.cx, "agent", "--host", "local", "--dir", src, "--inplace", "--interactive", "--tag", "conversation", "--idle-exit", "5s", "task")
	defer cliOutput(t, 0, nil, f.cx, "close", "local:p/conversation")
	if !bytes.Contains(out, []byte("STATUS: QUESTION")) || !bytes.Contains(out, []byte("JOB=p/conversation STATUS=QUESTION")) {
		t.Fatal(string(out))
	}
	out = cliOutput(t, 0, nil, f.cx, "agent", "--continue", "local:p/conversation", "second round")
	if !bytes.Contains(out, []byte("STATUS: DONE")) {
		t.Fatal(string(out))
	}
	past := cliOutput(t, 0, nil, f.cx, "result", "local:p/conversation", "--turn", "1", "--wait")
	if !bytes.Contains(past, []byte("Next?")) {
		t.Fatal(string(past))
	}
}
func TestB3ChatInteroperability(t *testing.T) {
	f := newControllerFixture(t)
	if f.python == "" {
		t.Log("cx-control.py unavailable: skipping chat interoperability comparisons")
	}
	t.Setenv("CX_CHAT_LOCAL_ROOT", filepath.Join(f.home, "chat"))
	row := cliOutput(t, 0, nil, f.cx, "chat", "post", "reports", "say @member")
	checkRead := func(exe string, args ...string) {
		t.Helper()
		data := cliOutput(t, 0, nil, exe, args...)
		var posted, read fields
		if json.Unmarshal(row, &posted) != nil || json.Unmarshal(data, &read) != nil || posted["text"] != "say @member" || !reflect.DeepEqual(posted, read) {
			t.Fatalf("chat post %s / read %s", row, data)
		}
	}
	checkRead(f.cx, "chat", "read", "reports")
	if f.python != "" {
		checkRead("python3", f.python, "chat", "read", "reports")
	}
	cliOutput(t, 0, nil, f.cx, "chat", "wait", "reports", "--mention", "member", "--timeout", "1s")
	if data := cliOutput(t, 0, nil, f.cx, "chat", "read", "reports", "--cursor", "default"); len(data) > 0 {
		t.Fatal(string(data))
	}
	if f.python != "" {
		if data := cliOutput(t, 0, nil, "python3", f.python, "chat", "read", "reports", "--cursor", "default"); len(data) > 0 {
			t.Fatal(string(data))
		}
	}
	waitBoth := func(want int, limit string) {
		t.Helper()
		args := []string{"wait", "--any", "--chat", "reports", "--timeout", limit}
		goOut := cliOutput(t, want, nil, f.cx, args...)
		if want == 124 || want == 9 {
			if len(goOut) != 0 {
				t.Fatalf("unexpected Go wait output: %s", goOut)
			}
			if f.python != "" {
				if pyOut := cliOutput(t, want, nil, "python3", append([]string{f.python}, args...)...); len(pyOut) != 0 {
					t.Fatalf("unexpected Python wait output: %s", pyOut)
				}
			}
			return
		}
		var goRow, pyRow fields
		if err := json.Unmarshal(goOut, &goRow); err != nil {
			t.Fatalf("Go wait output %q: %v", goOut, err)
		}
		if f.python == "" {
			return
		}
		pyOut := cliOutput(t, want, nil, "python3", append([]string{f.python}, args...)...)
		if json.Unmarshal(pyOut, &pyRow) != nil || !reflect.DeepEqual(goRow, pyRow) {
			t.Fatalf("wait: Go %s, Python %s", goOut, pyOut)
		}
	}
	waitBoth(0, "2s")
	cliOutput(t, 17, nil, f.cx, "exec", "local", "--tag", "failed", "--wait", "--", "sh", "-c", "exit 17")
	waitBoth(1, "2s")
	waitBoth(124, "300ms")
	cliOutput(t, 0, nil, f.cx, "chat", "post", "reports", "--type", "decision", "ship")
	if runtime.GOOS == "darwin" {
		d, e := os.ReadFile(filepath.Join(f.home, "out", "p", "decisions.jsonl"))
		if e != nil || !bytes.Contains(d, []byte("ship")) {
			t.Fatal(string(d), e)
		}
	}
	// One failed observer and one idle observer must preserve the failure.
	brokenRoot := filepath.Join(f.home, "broken")
	if err := os.MkdirAll(filepath.Join(brokenRoot, "p-meta.git"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CX_CHAT_LOCAL_ROOT", brokenRoot)
	waitBoth(9, "1s")
}

func TestB3LinuxCenterSetup(t *testing.T) {
	f := newControllerFixture(t)
	root := filepath.Join(f.home, "center")
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	target := filepath.Join(root, "cx")
	if e := os.Symlink(f.cx, target); e != nil {
		t.Fatal(e)
	}
	module := filepath.Join(f.home, "src")
	if e := os.Mkdir(module, 0700); e != nil {
		t.Fatal(e)
	}
	for n, s := range map[string]string{"go.mod": "module cx\n\ngo 1.23\n", "go.sum": "", "ctl_deploy.go": "package main\nimport \"fmt\"\nfunc main(){fmt.Println(\"built on center\")}\n"} {
		if e := os.WriteFile(filepath.Join(module, n), []byte(s), 0600); e != nil {
			t.Fatal(e)
		}
	}
	t.Setenv("CX_MODULE", module)
	t.Setenv("CX_SETUP_BINARY", "")
	t.Setenv("CX_TEST_CENTER", root)
	mock := `#!/bin/sh
set -eu
while test "$1" != --; do shift; done
shift 2
cd "$CX_TEST_CENTER"
export HOME="$PWD" CXRUN_STATE="$PWD/state"
exec /bin/sh -c "$1"
`
	if e := os.WriteFile(filepath.Join(f.home, "ssh"), []byte(mock), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	handled, e := deployFromCenter(ctx, endpoint{SSH: "center", CX: target, Platform: "linux/amd64"}, runtime.GOOS+"/"+runtime.GOARCH)
	if e != nil || !handled {
		t.Fatal(handled, e)
	}
	if got := cliOutput(t, 0, nil, target); string(got) != "built on center\n" {
		t.Fatal(string(got))
	}
}

func TestB3SubmissionShow(t *testing.T) {
	f := newControllerFixture(t)
	dir, e := submissionDir("local", "p/show")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	s := submissionProgress{State: "submitting", Project: "p", Job: "show", Phase: "packet", Bytes: 1024}
	if e = writeJSON(filepath.Join(dir, "submission.json"), s); e != nil {
		t.Fatal(e)
	}
	lock, e := lockPath(filepath.Join(dir, "worker.lock"), os.O_CREATE)
	if e != nil || lock == nil {
		t.Fatal(e)
	}
	show := func(want string) {
		t.Helper()
		var result submissionProgress
		if e := json.Unmarshal(cliOutput(t, 0, nil, f.cx, "show", "local:p/show", "--json"), &result); e != nil || result.State != want || result.Bytes != 1024 {
			t.Fatal(result, e)
		}
	}
	show("submitting")
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	show("submit_failed")
}

func TestB3ReuseCleanAndGate(t *testing.T) {
	f := newControllerFixture(t)
	dir := filepath.Join(f.home, "external")
	if e := os.MkdirAll(filepath.Join(dir, "scripts"), 0700); e != nil {
		t.Fatal(e)
	}
	a := []string{"exec", "local", "--cwd", dir, "--env", "CX_TEST_ENV=value", "--tag", "first", "--wait", "--", "sh", "-c", "echo \"$CX_TEST_ENV\"; exit 3"}
	if b := cliOutput(t, 3, nil, f.cx, a...); string(b) != "value\n" {
		t.Fatal(string(b))
	}
	a[7] = "second"
	a = append(a[:8], append([]string{"--reuse"}, a[8:]...)...)
	var hit reuseHit
	if e := json.Unmarshal(cliOutput(t, 3, nil, f.cx, a...), &hit); e != nil || !hit.Reused || hit.Job != "p/first" {
		t.Fatal(hit, e)
	}
	cliOutput(t, 0, nil, f.cx, "clean", "p/first")
	if _, e := os.Stat(dir); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(dir, "scripts", "gate.sh"), []byte("#!/bin/sh\nprintf '%s\\n' \"$LAN_SNAPSHOT_ID\" \"$1\"\n"), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("CX_DEFAULT_HOST", "local")
	out := cliOutput(t, 0, nil, f.cx, "gate", dir, "literal arg")
	parts := strings.Fields(string(out))
	if len(parts) != 3 || !snapHex(parts[0], 64) {
		t.Fatal(string(out))
	}
}

func TestB3DroppedSnapshotConnection(t *testing.T) {
	f := newControllerFixture(t)
	t.Setenv("CX_PUBLISH_RETRY_WINDOW", "0")
	root := filepath.Join(f.home, "src")
	if e := os.Mkdir(root, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(root, "note"), []byte("data"), 0600); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sid, _, e := Freeze(ctx, root, FreezeOptions{})
	if e != nil {
		t.Fatal(e)
	}
	remote := filepath.Join(f.home, "remote")
	if e = os.Mkdir(remote, 0700); e != nil {
		t.Fatal(e)
	}
	sh := filepath.Join(f.home, "ssh")
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	if e = os.WriteFile(sh, []byte("#!/bin/sh\nexit 255\n"), 0700); e != nil {
		t.Fatal(e)
	}
	h := endpoint{SSH: "fixture", CX: f.cx, State: remote, Platform: runtime.GOOS + "/" + runtime.GOARCH}
	if e = Publish(ctx, h, sid, nil); e == nil {
		t.Fatal("missing transport error")
	}
	store, e := snapStore()
	if e != nil {
		t.Fatal(e)
	}
	matches, e := filepath.Glob(filepath.Join(store, "published", "*", string(sid)+".tar"))
	if e != nil || len(matches) != 1 {
		t.Fatal(matches, e)
	}
	if e = os.WriteFile(sh, []byte("#!/bin/sh\nwhile test \"$1\" != --; do shift; done\nshift 2\nexec /bin/sh -c \"$1\"\n"), 0700); e != nil {
		t.Fatal(e)
	}
	if e = Publish(ctx, h, sid, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(matches[0]); !os.IsNotExist(e) {
		t.Fatal("staging archive retained after acknowledgement", e)
	}
}

func TestB3ResumeResultStream(t *testing.T) {
	f := newControllerFixture(t)
	cliOutput(t, 0, nil, f.cx, "exec", "local", "--tag", "download", "--wait", "--", "sh", "-c", "echo finished")
	dir, err := jobDir(os.Getenv("CXRUN_STATE"), "p/download")
	if err != nil {
		t.Fatal(err)
	}
	var s status
	if err = readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
		t.Fatal(err)
	}
	ssh := filepath.Join(f.home, "ssh")
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	// Forward the request locally. Drop the mock transport after 30 archive bytes.
	shim := "#!/bin/sh\nwhile test \"$1\" != --; do shift; done\nshift 2\n"
	drop := shim + "sh -c \"$1\" | { IFS= read -r size; printf '%s\\n' \"$size\"; dd bs=1 count=30 2>/dev/null; }\nexit 255\n"
	if err = os.WriteFile(ssh, []byte(drop), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	dest := filepath.Join(f.home, "received")
	h := endpoint{SSH: "fixture", CX: f.cx, State: os.Getenv("CXRUN_STATE"), Platform: runtime.GOOS + "/" + runtime.GOARCH}
	if err = fetchTurn(ctx, h, "p/download", s.RunID, "1", dest, true); !errors.Is(err, ctlExit(9)) {
		t.Fatal(err)
	}
	fi, err := os.Stat(dest + ".part")
	if err != nil || fi.Size() != 30 {
		t.Fatal(fi, err)
	}
	if err = os.WriteFile(ssh, []byte(shim+"exec sh -c \"$1\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = fetchTurn(ctx, h, "p/download", s.RunID, "1", dest, true); err != nil {
		t.Fatal(err)
	}
	if report, err := os.ReadFile(filepath.Join(dest, "last.md")); err != nil || !bytes.Contains(report, []byte("finished")) {
		t.Fatal(string(report), err)
	}
	if log, err := os.ReadFile(filepath.Join(dest, "output.log")); err != nil || string(log) != "finished\n" {
		t.Fatal(string(log), err)
	}
}

func TestB3LegacySubmitAndClean(t *testing.T) {
	f := newControllerFixture(t)
	root := os.Getenv("CXRUN_STATE")
	work := filepath.Join(f.home, ".locks", "cx-executor", "cx-v2", "jobs", "p", "old")
	token := snapSum([]byte("fixture registration"))
	legacy := fields{"host": "local", "state": root, "work": work, "project": "p", "tag": "old", "request": token, "submit_status": "submit_failed", "submit_reason": "fixture", "submit_rc": 8}
	route, e := legacyRouteFile("p/old")
	if e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{route, filepath.Join(work, "request.json")} {
		if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
			t.Fatal(e)
		}
		if e = writeJSON(path, legacy); e != nil {
			t.Fatal(e)
		}
	}
	var view submissionProgress
	if e = json.Unmarshal(cliOutput(t, 0, nil, f.cx, "show", "p/old"), &view); e != nil || view.State != "submit_failed" || view.Code == nil || *view.Code != 8 {
		t.Fatal(view, e)
	}
	cliOutput(t, 0, nil, f.cx, "clean", "p/old")
	for _, path := range []string{route, work} {
		if _, e = os.Stat(path); !os.IsNotExist(e) {
			t.Fatal(path, e)
		}
	}
}

func TestB3MirrorRef(t *testing.T) {
	f := newControllerFixture(t)
	dir := filepath.Join(f.home, "input")
	cliOutput(t, 0, nil, "git", "init", "-q", "-b", "topic", dir)
	t.Setenv("GIT_AUTHOR_NAME", "fixture")
	t.Setenv("GIT_AUTHOR_EMAIL", "fixture")
	t.Setenv("GIT_COMMITTER_NAME", "fixture")
	t.Setenv("GIT_COMMITTER_EMAIL", "fixture")
	cliOutput(t, 0, nil, "git", "-C", dir, "commit", "--allow-empty", "-qm", "base")
	id := strings.TrimSpace(string(cliOutput(t, 0, nil, f.cx, "mirror", dir)))
	m, e := loadSnapshot(SnapshotID(id))
	if e != nil {
		t.Fatal(e)
	}
	store, e := snapStore()
	if e != nil {
		t.Fatal(e)
	}
	r := m.Repositories[0]
	ref := strings.TrimSpace(string(cliOutput(t, 0, nil, "git", "-C", r.bare(store), "rev-parse", "refs/heads/topic")))
	if ref != *r.Head {
		t.Fatal(ref, r.Head)
	}
}

func TestB3EmbeddedPolicies(t *testing.T) {
	t.Setenv("CX_POLICY_DIR", t.TempDir())
	for _, name := range []string{"preamble.md", "interactive.md", "stall-nudge.md"} {
		got, e := controllerPolicy(name)
		if e != nil {
			t.Fatal(e)
		}
		if strings.TrimSpace(got) == "" {
			t.Fatalf("empty embedded policy %s", name)
		}
	}
}

func TestB3CenterWithoutGoUsesDelta(t *testing.T) {
	f := newControllerFixture(t)
	if _, e := exec.LookPath("rsync"); e != nil {
		t.Skip("rsync unavailable")
	}
	center := filepath.Join(f.home, "center")
	if e := os.Mkdir(center, 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("CX_FIXTURE_CENTER", center)
	t.Setenv("CX_FIXTURE_CX", f.cx)
	t.Setenv("PATH", f.home+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("CX_PUBLISH_RETRY_WINDOW", "0")
	ssh := `#!/bin/sh
set -eu
cd "$CX_FIXTURE_CENTER"
while test "$1" != fixture; do shift; done
shift
case "$1" in
  "command -v go"*) exit 42 ;;
  rsync) cmp cx .cx-delta-*; exec "$@" ;;
  *) exec /bin/sh -c "$1" ;;
esac
`
	if e := os.WriteFile(filepath.Join(f.home, "ssh"), []byte(ssh), 0700); e != nil {
		t.Fatal(e)
	}
	target := filepath.Join(center, "cx")
	data := []byte("#!/bin/sh\nexec \"$CX_FIXTURE_CX\" \"$@\" #old\n")
	if e := os.WriteFile(target, data, 0700); e != nil {
		t.Fatal(e)
	}
	next := filepath.Join(f.home, "new-cx")
	data = bytes.Replace(data, []byte("#old"), []byte("#new"), 1)
	if e := os.WriteFile(next, data, 0700); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()
	h := endpoint{SSH: "fixture", CX: target, State: filepath.Join(center, "state"), Platform: "linux/amd64"}
	if handled, e := deployFromCenter(ctx, h, h.Platform); e != nil || handled {
		t.Fatal(handled, e)
	}
	if e := deltaInstall(ctx, h, next, snapSum(data)); e != nil {
		t.Fatal(e)
	}
	if got, e := os.ReadFile(target); e != nil || !bytes.Equal(data, got) {
		t.Fatal(string(got), e)
	}
}

func TestB3SessionScope(t *testing.T) {
	f := newControllerFixture(t)
	if f.python == "" {
		t.Log("cx-control.py unavailable: skipping session listing comparisons")
	}
	t.Setenv("CODEX_THREAD_ID", "one")
	cliOutput(t, 0, nil, f.cx, "exec", "local", "--tag", "one", "--wait", "--", "true")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "nested")
	t.Setenv("CLAUDE_CODE_HOST_SESSION_ID", "two")
	cliOutput(t, 0, nil, f.cx, "exec", "local", "--tag", "two", "--wait", "--", "true")
	for _, tc := range []struct {
		want map[string]string
		args []string
	}{
		{want: map[string]string{"two": "two"}},
		{args: []string{"--all"}, want: map[string]string{"one": "one", "two": "two"}},
		{args: []string{"--session", "one"}, want: map[string]string{"one": "one"}},
	} {
		args := append([]string{"ls", "--json"}, tc.args...)
		goOut := cliOutput(t, 0, nil, f.cx, args...)
		parse := func(b []byte) map[string]string {
			v := map[string]string{}
			for _, line := range bytes.Split(bytes.TrimSpace(b), []byte("\n")) {
				var row status
				if e := json.Unmarshal(line, &row); e != nil {
					t.Fatal(e)
				}
				v[row.Job] = row.Session
			}
			return v
		}
		got := parse(goOut)
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%v: Go %v, want %v", args, got, tc.want)
		}
		if f.python != "" {
			pyOut := cliOutput(t, 0, nil, "python3", append([]string{f.python}, args...)...)
			if py := parse(pyOut); !reflect.DeepEqual(got, py) {
				t.Fatalf("%v: Go %v, Python %v", args, got, py)
			}
		}
	}
}

func TestB3RetryWindow(t *testing.T) {
	t.Setenv("CX_PUBLISH_RETRY_WINDOW", "6.5")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	attempts := 0
	started := time.Now()
	err := retryTransfer(ctx, func() error { attempts++; return ctlExit(9) })
	if !errors.Is(err, ctlExit(9)) || attempts != 2 || time.Since(started) < 5*time.Second {
		t.Fatal(attempts, err)
	}
	attempts = 0
	invalid := errors.New("invalid manifest")
	err = retryTransfer(ctx, func() error { attempts++; return invalid })
	if !errors.Is(err, invalid) || attempts != 1 {
		t.Fatal(attempts, err)
	}
}
