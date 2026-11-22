package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSnapHistory(t *testing.T) {
	ctx := snapTestCtx(t)
	reference, hasReference := pythonReference(t, filepath.Join("snapshot", "freeze.py"))
	if hasReference {
		t.Setenv("PYTHONPATH", filepath.Dir(reference))
	} else {
		t.Log("snapshot/freeze.py unavailable: skipping history snapshot ID comparisons")
	}
	src := t.TempDir()
	g := snapGit{ctx: ctx, dir: src}
	git := func(a ...string) string {
		t.Helper()
		s, e := g.get(a...)
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	git("init", "-q")
	for i := 0; i < 3; i++ {
		snapWriteTest(t, filepath.Join(src, "tracked.pem"), fmt.Sprint(i))
		git("add", ".")
		git("-c", "user.name=fixture", "-c", "user.email=fixture@invalid", "commit", "-qm", "version")
	}
	hub := endpoint{State: t.TempDir()}
	for _, history := range []string{"HEAD~1..HEAD", "HEAD"} {
		id, m, e := Freeze(ctx, src, FreezeOptions{History: history})
		if e != nil {
			t.Fatal(e)
		}
		if hasReference {
			cmd := exec.CommandContext(ctx, "python3", "-c", `from pathlib import Path
import freeze,sys
freeze.STORE=Path(sys.argv[2])
print(freeze.freeze(Path(sys.argv[1]),history=sys.argv[3]))`, src, t.TempDir(), history)
			var diag bytes.Buffer
			cmd.Stderr = &diag
			py, err := cmd.Output()
			if err != nil || strings.TrimSpace(string(py)) != string(id) {
				t.Fatalf("history differential: %v python=%s Go=%s %s", err, py, id, diag.String())
			}
		}
		row := m.Repositories[0]
		if row.Base != git("rev-parse", "HEAD") || row.Head == nil || row.Base != *row.Head {
			t.Fatal("wrong base")
		}
		if e = Publish(ctx, hub, id, nil); e != nil {
			t.Fatal(e)
		}
		out, e := Materialize(ctx, t.TempDir(), m, filepath.Join(hub.State, "snapshot"))
		if e != nil {
			t.Fatal(e)
		}
		n, e := (snapGit{ctx: ctx, dir: out.Source}).get("rev-list", "--count", "HEAD")
		if e != nil || history == "HEAD" && n != "3" || history != "HEAD" && n != "2" {
			t.Fatalf("history %s: %s %v", history, n, e)
		}
	}
}
func TestSnapResume(t *testing.T) {
	ctx := snapTestCtx(t)
	src := t.TempDir()
	snapWriteTest(t, filepath.Join(src, "file"), "data")
	sid, _, e := Freeze(ctx, src, FreezeOptions{})
	if e != nil {
		t.Fatal(e)
	}
	store, _ := snapStore()
	work := t.TempDir()
	packet := filepath.Join(work, "packet")
	if e = snapArchive(ctx, store, string(sid), work, packet, false); e != nil {
		t.Fatal(e)
	}
	b, e := os.ReadFile(packet)
	if e != nil {
		t.Fatal(e)
	}
	dest := t.TempDir()
	args := []string{string(sid), snapSum(b), strconv.Itoa(len(b))}
	send := func(data []byte) (string, error) {
		var wire, ack bytes.Buffer
		z := gzip.NewWriter(&wire)
		_, receiveErr := z.Write(data)
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		if receiveErr = z.Close(); receiveErr != nil {
			t.Fatal(receiveErr)
		}
		receiveErr = snapReceiveAt(ctx, dest, args, &wire, &ack)
		line, _, _ := strings.Cut(ack.String(), "\n")
		return line, receiveErr
	}
	split := len(b) / 2
	if off, sendErr := send(b[:split]); sendErr == nil || off != "0" {
		t.Fatalf("partial: %s %v", off, sendErr)
	}
	if _, _, e = snapManifest(dest, string(sid)); !os.IsNotExist(e) {
		t.Fatalf("early publication: %v", e)
	}
	if off, e := send(b[split:]); e != nil || off != strconv.Itoa(split) {
		t.Fatalf("resume: %s %v", off, e)
	}
	if off, e := send(nil); e != nil || off != "done" {
		t.Fatalf("repeat: %s %v", off, e)
	}
}

// Exercise the normal B1 wire entry point in a child cx process, including
// stdin ownership. Direct calls to the importer cannot detect a routing error.
func TestSnapNodeWire(t *testing.T) {
	ctx := snapTestCtx(t)
	store, _ := snapStore()
	src := t.TempDir()
	snapWriteTest(t, filepath.Join(src, "note"), "transfer")
	id, _, e := Freeze(ctx, src, FreezeOptions{})
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "offer")
	if e = snapArchive(ctx, store, string(id), dir, p, false); e != nil {
		t.Fatal(e)
	}
	hash, e := fileHash(p)
	if e != nil {
		t.Fatal(e)
	}
	st, e := os.Stat(p)
	if e != nil {
		t.Fatal(e)
	}
	target := t.TempDir()
	t.Setenv("CX_SNAPSHOT_STORE", target)
	t.Setenv("GORACE", "atexit_sleep_ms=1")
	child, stop := context.WithTimeout(ctx, 4*time.Second)
	defer stop()
	var got int64
	if e = snapSend(child, endpoint{}, []string{string(id), hash, strconv.FormatInt(st.Size(), 10)}, p, func(n int64) { got = n }); e != nil {
		t.Fatalf("cx node wire must dispatch snap-receive to ReceiveSnapshot and preserve streaming stdin: %v", e)
	}
	if got != st.Size() {
		t.Fatalf("progress %d, expected %d", got, st.Size())
	}
	if _, _, e = snapManifest(target, string(id)); e != nil {
		t.Fatal(e)
	}
}
