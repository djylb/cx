//go:build darwin || linux

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/sys/unix"
)

func TestCommandInputPreservesInheritedDescriptor(t *testing.T) {
	const idle = 100 * time.Millisecond
	if os.Getenv("CX_TEST_STDIN_CHILD") == "1" {
		in := commandInput{ctx: context.Background(), idle: idle}
		synctest.Test(t, func(t *testing.T) {
			var p [1]byte
			for _, want := range []byte{'x', 'y'} {
				if _, e := io.ReadFull(in, p[:]); e != nil || p[0] != want {
					t.Fatalf("read across idle boundary: got %q, %v, want %q", p[0], e, want)
				}
				// Local processing can take an entire idle period while no
				// read is pending. The next read gets its own deadline.
				time.Sleep(idle)
			}
		})
		if _, e := io.ReadAll(in); !errors.Is(e, context.DeadlineExceeded) {
			t.Fatalf("silent input: %v", e)
		}
		if _, e := unix.FcntlInt(uintptr(unix.Stdin), unix.F_GETFL, 0); e != nil {
			t.Fatalf("stdin was closed: %v", e)
		}
		return
	}
	var fds [2]int
	// Keep our copy of the child's blocking input open and inspect its flags.
	if e := unix.Pipe(fds[:]); e != nil {
		t.Fatal(e)
	}
	r, w := os.NewFile(uintptr(fds[0]), "r"), os.NewFile(uintptr(fds[1]), "w")
	defer closeBestEffort(r)
	defer closeBestEffort(w)
	if _, e := w.WriteString("xy"); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := fixtureContext(t)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCommandInputPreservesInheritedDescriptor$")
	cmd.Env = append(os.Environ(), "CX_TEST_STDIN_CHILD=1")
	cmd.Stdin = r
	if out, e := cmd.CombinedOutput(); e != nil {
		t.Fatalf("silent stdin child: %v: %s", e, out)
	}
	flags, e := unix.FcntlInt(r.Fd(), unix.F_GETFL, 0)
	if e != nil || flags&unix.O_NONBLOCK != 0 {
		t.Fatalf("inherited stdin flags changed: %x, %v", flags, e)
	}
}

func TestCommandInputNamedFIFOEOF(t *testing.T) {
	if os.Getenv("CX_TEST_FIFO_CHILD") == "1" {
		in := commandInput{ctx: context.Background(), idle: fixtureLimit}
		var p [1]byte
		if _, e := io.ReadFull(in, p[:]); e != nil || p[0] != 'x' {
			t.Fatalf("FIFO data: %q, %v", p[:], e)
		}
		if _, err := fmt.Fprintln(os.Stdout, "read"); err != nil {
			t.Fatal(err)
		}
		if _, e := in.Read(p[:]); e != io.EOF {
			t.Fatalf("FIFO hangup: %v", e)
		}
		return
	}
	p := filepath.Join(t.TempDir(), "stdin")
	if e := unix.Mkfifo(p, 0600); e != nil {
		t.Fatal(e)
	}
	fd, e := unix.Open(p, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if e != nil {
		t.Fatal(e)
	}
	if e = unix.SetNonblock(fd, false); e != nil {
		_ = unix.Close(fd)
		t.Fatal(e)
	}
	r := os.NewFile(uintptr(fd), p)
	defer closeBestEffort(r)
	w, e := os.OpenFile(p, os.O_WRONLY|unix.O_NONBLOCK, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer closeBestEffort(w)
	if _, e = w.WriteString("x"); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := fixtureContext(t)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCommandInputNamedFIFOEOF$")
	cmd.Env = append(os.Environ(), "CX_TEST_FIFO_CHILD=1")
	cmd.Stdin = r
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, e := cmd.StdoutPipe()
	if e != nil {
		t.Fatal(e)
	}
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cancel()
			_ = cmd.Wait()
		}
	}()
	scan := bufio.NewReader(out)
	if line, readErr := scan.ReadString('\n'); readErr != nil || line != "read\n" {
		t.Fatalf("FIFO child did not confirm data: %q, %v", line, readErr)
	}
	if e = w.Close(); e != nil {
		t.Fatal(e)
	}
	rest, e := io.ReadAll(scan)
	if e != nil {
		t.Fatal(e)
	}
	if e = cmd.Wait(); e != nil {
		t.Fatalf("FIFO child: %v: %s%s", e, rest, &stderr)
	}
}

func TestSnapshotLockDoesNotSpendReadIdleBudget(t *testing.T) {
	const idle = 100 * time.Millisecond
	if raw := os.Getenv("CX_TEST_LOCKED_SNAPSHOT"); raw != "" {
		args := strings.Fields(raw)
		store, e := snapStore()
		if e != nil {
			t.Fatal(e)
		}
		dir := filepath.Join(store, "incoming")
		if e = os.MkdirAll(dir, 0700); e != nil {
			t.Fatal(e)
		}
		lock, e := lockPath(filepath.Join(dir, args[0]+".lock"), os.O_CREATE)
		if e != nil || lock == nil {
			t.Fatalf("hold snapshot lock: %v", e)
		}
		synctest.Test(t, func(t *testing.T) {
			go func() {
				time.Sleep(5 * idle)
				closeBestEffort(lock)
			}()
			ctx, cancel := fixtureContext(t)
			defer cancel()
			if e := dispatch(ctx, append([]string{"node", "snap-receive"}, args...)); e != nil {
				t.Fatal(e)
			}
		})
		return
	}

	ctx := snapTestCtx(t)
	src := t.TempDir()
	snapWriteTest(t, filepath.Join(src, "fixture"), "packet data")
	id, _, e := Freeze(ctx, src, FreezeOptions{})
	if e != nil {
		t.Fatal(e)
	}
	store, e := snapStore()
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "packet")
	if e = snapArchive(ctx, store, string(id), dir, p, false); e != nil {
		t.Fatal(e)
	}
	packet, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	target := t.TempDir()
	t.Setenv("CX_SNAPSHOT_STORE", target)
	t.Setenv("CX_TEST_LOCKED_SNAPSHOT", strings.Join([]string{string(id), snapSum(packet), strconv.Itoa(len(packet)), idle.String()}, " "))
	var wire bytes.Buffer
	z := gzip.NewWriter(&wire)
	_, e = z.Write(packet)
	if e = errors.Join(e, z.Close()); e != nil {
		t.Fatal(e)
	}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSnapshotLockDoesNotSpendReadIdleBudget$")
	cmd.Stdin = &wire
	if out, runErr := cmd.CombinedOutput(); runErr != nil {
		t.Fatalf("snapshot receiver: %v: %s", runErr, out)
	}
	if _, _, e = snapManifest(target, string(id)); e != nil {
		t.Fatal(e)
	}
}

func TestSnapshotReceiverIdleIsRetryable(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
	}{
		{"silent input", 9},
		{"closed input", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CX_SNAPSHOT_STORE", t.TempDir())
			ctx, cancel := fixtureContext(t)
			defer cancel()
			r, w, e := os.Pipe()
			if e != nil {
				t.Fatal(e)
			}
			defer closeBestEffort(r)
			defer closeBestEffort(w)
			if tc.name == "closed input" {
				closeBestEffort(w)
			}
			cmd := exec.CommandContext(ctx, os.Args[0], "node", "snap-receive",
				strings.Repeat("0", 64), strings.Repeat("1", 64), "1", "100ms")
			cmd.Stdin = r
			out, e := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(e, &exit) || exit.ExitCode() != tc.code {
				t.Fatalf("expected exit %d, got %v: %s", tc.code, e, out)
			}
		})
	}
}
