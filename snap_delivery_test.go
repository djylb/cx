package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestSnapHistory(t *testing.T) {
	ctx := snapTestCtx(t)
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
	var commits []string
	for _, content := range []string{"0", "1", "2"} {
		snapWriteTest(t, filepath.Join(src, "tracked.pem"), content)
		git("add", ".")
		git("-c", "user.name=fixture", "-c", "user.email=fixture@invalid", "commit", "-qm", "version")
		commits = append(commits, git("rev-parse", "HEAD"))
	}
	snapWriteTest(t, filepath.Join(src, "tracked.pem"), "working version")
	hub := endpoint{State: t.TempDir()}
	for _, tc := range []struct {
		history string
		shallow []string
		chain   []string
	}{
		{"HEAD~1..HEAD", []string{commits[1]}, []string{commits[2], commits[1]}},
		{"HEAD", nil, []string{commits[2], commits[1], commits[0]}},
	} {
		id, m, e := Freeze(ctx, src, FreezeOptions{History: tc.history})
		if e != nil {
			t.Fatal(e)
		}
		if len(m.Repositories) != 1 {
			t.Fatalf("history %s repositories: %+v", tc.history, m.Repositories)
		}
		row := m.Repositories[0]
		if !row.History || row.Base != commits[2] || row.Head == nil || *row.Head != commits[2] ||
			!slices.Equal(row.Shallow, tc.shallow) {
			t.Fatalf("history %s repository: %+v", tc.history, row)
		}
		if e = Publish(ctx, hub, id, nil); e != nil {
			t.Fatal(e)
		}
		out, e := Materialize(ctx, t.TempDir(), m, filepath.Join(hub.State, "snapshot"))
		if e != nil {
			t.Fatal(e)
		}
		chain, e := (snapGit{ctx: ctx, dir: out.Source}).get("rev-list", "HEAD")
		want := strings.Join(tc.chain, "\n")
		if e != nil || chain != want {
			t.Fatalf("history %s: got %q (%v), want %q", tc.history, chain, e, want)
		}
		if b, err := os.ReadFile(filepath.Join(out.Source, "tracked.pem")); err != nil || string(b) != "working version" {
			t.Fatalf("history %s checkout: got %q (%v), want working version", tc.history, b, err)
		}
	}
}
func TestSnapResume(t *testing.T) {
	ctx := snapTestCtx(t)
	src := t.TempDir()
	snapWriteTest(t, filepath.Join(src, "file"), "data")
	sid, manifest, e := Freeze(ctx, src, FreezeOptions{})
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
	repo := manifest.Repositories[0]
	if e = os.Remove(filepath.Join(repo.bare(dest), "refs", "snapshots", repo.OID)); e != nil {
		t.Fatal(e)
	}
	if off, e := send(b); e != nil || off != "0" {
		t.Fatalf("repair a manifest with missing refs: %s %v", off, e)
	}
}

func TestSnapWireDisconnectReleasesSnapshotLock(t *testing.T) {
	for _, tc := range []struct {
		name       string
		idle       time.Duration
		keepWriter bool
		want       int
	}{
		{"closed sender pipe", time.Minute, false, 1},
		{"inherited sender handle remains", 250 * time.Millisecond, true, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
			defer cancel()
			store := t.TempDir()
			t.Setenv("CX_SNAPSHOT_STORE", store)
			t.Setenv("GORACE", "atexit_sleep_ms=1")
			id := strings.Repeat("0", 64)
			digest := strings.Repeat("1", 64)
			args, err := json.Marshal(invocation{Args: []string{"snap-receive", id, digest, "16", tc.idle.String()}})
			if err != nil {
				t.Fatal(err)
			}
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer closeBestEffort(r)
			defer closeBestEffort(w)
			cmd := exec.CommandContext(ctx, os.Args[0], "node", "wire", base64.RawURLEncoding.EncodeToString(args))
			cmd.Stdin = r
			out, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if cmd.ProcessState == nil {
					cancel()
					_ = cmd.Wait()
				}
			}()
			scanner := bufio.NewReader(out)
			if line, err := scanner.ReadString('\n'); line != "0\n" || err != nil {
				t.Fatalf("snapshot receiver handshake: %q, %v", line, err)
			}
			lockName := filepath.Join(store, "incoming", id+".lock")
			if !tc.keepWriter {
				// Observe a partial body and the lock before closing stdin.
				z := gzip.NewWriter(w)
				if _, err := z.Write([]byte("x")); err != nil {
					t.Fatal(err)
				}
				if err := z.Flush(); err != nil {
					t.Fatal(err)
				}
				if line, err := scanner.ReadString('\n'); line != "bytes 1\n" || err != nil {
					t.Fatalf("snapshot receiver partial body: %q, %v", line, err)
				}
				lock, err := lockPath(lockName, 0)
				if err != nil || lock != nil {
					if lock != nil {
						closeBestEffort(lock)
					}
					t.Fatalf("expected receiver to hold snapshot lock: %v", err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := io.Copy(io.Discard, scanner); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != tc.want {
				t.Fatalf("want exit %d, got %v: %s", tc.want, err, stderr.String())
			}
			lock, err := lockUntil(ctx, lockName)
			if err != nil {
				t.Fatalf("snapshot lock still held after disconnect: %v", err)
			}
			closeBestEffort(lock)
		})
	}
}

func TestSnapshotPipeDoesNotReadPastBufferedData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		haveData := true
		peek := func() (uint32, error) {
			if haveData {
				return 1, nil
			}
			// A disconnected sender leaked an inherited writer. The OS
			// cannot return EOF and a synchronous read would still block.
			return 0, nil
		}
		read := func(p []byte) (int, error) {
			if !haveData || len(p) != 1 {
				t.Fatalf("unbuffered synchronous read would block: data %v, len %d", haveData, len(p))
			}
			p[0], haveData = 'x', false
			return 1, nil
		}
		var buf [8]byte
		if n, err := readReadyPipe(ctx, buf[:], peek, read); n != 1 || buf[0] != 'x' || err != nil {
			t.Fatalf("buffered data read: %d, %q, %v", n, buf[0], err)
		}
		if n, err := readReadyPipe(ctx, buf[:], peek, read); n != 0 || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("idle read: %d, %v", n, err)
		}
	})
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

func TestSnapshotObjectVolumeFaultPreventsPublication(t *testing.T) {
	ctx := snapTestCtx(t)
	src := t.TempDir()
	snapWriteTest(t, filepath.Join(src, "payload"), "object-volume fixture")
	id, m, err := Freeze(ctx, src, FreezeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store, err := snapStore()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	packet := filepath.Join(dir, "packet")
	if err := snapArchive(ctx, store, string(id), dir, packet, true); err != nil {
		t.Fatal(err)
	}
	for _, failed := range []string{"object file", "ref directory"} {
		t.Run(failed, func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "separate-object-volume")
			if err := os.Mkdir(target, 0700); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("object volume sync failure")
			err := snapImportWithSync(ctx, target, string(id), packet, func(bare string) error {
				held, err := lockPath(bare+".lock", 0)
				if held != nil {
					closeBestEffort(held)
				}
				if held != nil || err != nil {
					t.Fatalf("object store sync runs outside its import lock: %v", err)
				}
				return syncSnapshotRepoWith(bare, func(f *os.File) error {
					if failed == "object file" && filepath.Ext(f.Name()) == ".pack" {
						return injected
					}
					return syncJobFile(f)
				}, func(path string) error {
					if failed == "ref directory" && path == filepath.Join(bare, "refs", "snapshots") {
						return injected
					}
					return syncJobDir(path)
				})
			})
			if !errors.Is(err, injected) {
				t.Fatalf("import should fail at %s barrier: %v", failed, err)
			}
			if _, _, err := snapManifest(target, string(id)); !os.IsNotExist(err) {
				t.Fatalf("manifest visible before object store durability: %v", err)
			}
			if err := snapImport(ctx, target, string(id), packet); err != nil {
				t.Fatal(err)
			}
			if _, err := Materialize(ctx, t.TempDir(), m, target); err != nil {
				t.Fatalf("successful retry did not publish usable objects: %v", err)
			}
		})
	}
}

func TestSnapshotRepublishRepairsObjectsBeforePreparedReuse(t *testing.T) {
	ctx := snapTestCtx(t)
	src := t.TempDir()
	snapWriteTest(t, filepath.Join(src, "payload"), "initial checkout contents")
	id, m, err := Freeze(ctx, src, FreezeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	hub := endpoint{State: t.TempDir()}
	target := filepath.Join(hub.State, "snapshot")
	if err := Publish(ctx, hub, id, nil); err != nil {
		t.Fatal(err)
	}
	job := t.TempDir()
	prepared, err := Materialize(ctx, job, m, target)
	if err != nil {
		t.Fatal(err)
	}
	payload := filepath.Join(prepared.Source, "payload")
	snapWriteTest(t, payload, "work in progress in surviving checkout")
	bare := m.Repositories[0].bare(target)
	packs, err := filepath.Glob(filepath.Join(bare, "objects", "pack", "*.pack"))
	if err != nil || len(packs) != 1 {
		t.Fatalf("fixture should have one imported pack: %v, %v", packs, err)
	}
	// Keep the manifest, refs and the pack index. A separate-volume drop
	// can leave any combination of files present.
	if err := os.Remove(packs[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := Materialize(ctx, job, m, target); err == nil {
		t.Fatal("reused prepared checkout with missing Git objects")
	}
	if err := Publish(ctx, hub, id, nil); err != nil {
		t.Fatalf("republish should try a full bundle after prerequisites fail: %v", err)
	}
	if _, err := Materialize(ctx, job, m, target); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(payload); err != nil || string(data) != "work in progress in surviving checkout" {
		t.Fatalf("object store repair replaced a previously prepared checkout: %q, %v", data, err)
	}
}

func TestSnapshotCheckoutVolumeFaultPreventsPreparedReuse(t *testing.T) {
	ctx := snapTestCtx(t)
	src := t.TempDir()
	snapWriteTest(t, filepath.Join(src, "payload"), "checkout-volume fixture")
	_, m, err := Freeze(ctx, src, FreezeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	store, err := snapStore()
	if err != nil {
		t.Fatal(err)
	}
	job := filepath.Join(t.TempDir(), "separate-checkout-volume")
	raw, err := snapJSON(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapImmutable(filepath.Join(job, "input", "manifest.json"), raw); err != nil {
		t.Fatal(err)
	}
	repo := m.Repositories[0]
	dst := filepath.Join(job, "work", "tree", repo.Path)
	if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
		t.Fatal(err)
	}
	mirror := snapGit{ctx: ctx, dir: repo.bare(store)}
	if err := mirror.worktree("add", "-q", "--detach", dst, repo.OID); err != nil {
		t.Fatal(err)
	}
	damaged := filepath.Join(dst, "payload")
	w, err := openStateLog(job)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(w)
	record, err := newRecord("blob", blobRef{Role: preparedBlobRole, SHA256: strings.Repeat("0", 64)})
	if err != nil {
		t.Fatal(err)
	}
	fault := errors.New("checkout volume sync failure")
	w.syncFn = func(f *os.File) error {
		if f.Name() == damaged {
			return fault
		}
		return syncJobFile(f)
	}
	if err := w.commit(record); !errors.Is(err, fault) {
		t.Fatalf("expected checkout barrier failure: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Discard the unsynced file while leaving the Git objects intact.
	if err := os.Remove(damaged); err != nil {
		t.Fatal(err)
	}
	if _, published, err := readPreparedSnapshot(ctx, job); published || err != nil {
		t.Fatalf("reused incomplete checkout: published %v, %v", published, err)
	}
	prepared, err := Materialize(ctx, job, m, store)
	if err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(prepared.Source, "payload")); err != nil || string(data) != "checkout-volume fixture" {
		t.Fatalf("checkout retry did not restore the lost file: %q, %v", data, err)
	}
}
