//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sshRecoveryFixture(t *testing.T, action string) string {
	t.Helper()
	dir := t.TempDir()
	script := `#!/bin/sh
set -eu
path=
while [ "$1" != -- ]; do
  case "$1" in
    -O) touch "$CX_SSH_FIXTURE/stop"; exit 1 ;;
    -oControlPath=*) path=${1#*=} ;;
  esac
  shift
done
shift
host=$1
shift
printf '%s\n' "$path" >> "$CX_SSH_FIXTURE/$host.paths"
` + action
	if e := os.WriteFile(filepath.Join(dir, "ssh"), []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("HOME", dir)
	t.Setenv("CX_SSH_FIXTURE", dir)
	return dir
}

func fixtureMaster(t *testing.T, ctx context.Context, alias string) sshMaster {
	t.Helper()
	_, master, e := endpointSSHOptions(ctx, alias)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = os.RemoveAll(filepath.Dir(master.state)) })
	return master
}

func TestSSHCallerDeadlineKeepsMaster(t *testing.T) {
	dir := sshRecoveryFixture(t, `printf 'still connecting\n' >&2
exec sleep 30
`)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	old := fixtureMaster(t, ctx, "worker")
	h := endpoint{SSH: "worker", CX: "cx"}
	if _, e := h.call(ctx, 100*time.Millisecond, "facts"); !errors.Is(e, context.DeadlineExceeded) || !errors.Is(e, ctlExit(9)) {
		t.Fatalf("caller deadline error = %v", e)
	}
	paths, e := os.ReadFile(filepath.Join(dir, "worker.paths"))
	if e != nil || len(paths) == 0 {
		t.Fatalf("SSH did not start: %q, %v", paths, e)
	}
	if _, e = os.Stat(filepath.Join(dir, "stop")); !os.IsNotExist(e) {
		t.Fatalf("attempted to stop an SSH master: %v", e)
	}
	if now := fixtureMaster(t, context.Background(), "worker"); now.generation != old.generation {
		t.Fatalf("caller deadline changed the generation: %v -> %v", old, now)
	}
}

func TestSSHRelayRecoveryByHop(t *testing.T) {
	for _, wedged := range []string{"first", "worker"} {
		t.Run(wedged, func(t *testing.T) {
			dir := sshRecoveryFixture(t, `
if [ "$host" = "$CX_WEDGED" ]; then
  initial=$(head -n 1 "$CX_SSH_FIXTURE/$host.paths")
  if [ "$path" = "$initial" ]; then
    printf 'cx-wire: ready\n' >&2
    exec sleep 30
  fi
fi
if [ "$host" = first ]; then
  export HOME="$CX_SSH_FIXTURE/relay-home"
  exec /bin/sh -c "$*"
fi
printf 'new connection\n'
`)
			t.Setenv("CX_WEDGED", wedged)
			// The relay hop re-executes this test binary; race builds start slowly, so the
			// idle budget must exceed one relay startup while staying far below the wedge.
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			exe, e := os.Executable()
			if e != nil {
				t.Fatal(e)
			}
			relayBin := filepath.Join(dir, "cx")
			if e = os.Symlink(exe, relayBin); e != nil {
				t.Fatal(e)
			}
			direct := fixtureMaster(t, ctx, "worker")
			first := fixtureMaster(t, ctx, "first")
			t.Setenv("HOME", filepath.Join(dir, "relay-home"))
			fixtureMaster(t, ctx, "worker")
			t.Setenv("HOME", dir)
			h := endpoint{SSH: "worker", CX: "cx", relay: &endpoint{SSH: "first", CX: relayBin}}
			r, e := h.call(ctx, 3*time.Second, "facts")
			if e != nil || r.code != 0 || string(r.data) != "new connection\n" {
				t.Fatalf("replay got %q, exit %d, error %v", r.data, r.code, e)
			}
			for _, host := range []string{"first", "worker"} {
				b, e := os.ReadFile(filepath.Join(dir, host+".paths"))
				if e != nil {
					t.Fatal(e)
				}
				paths := strings.Fields(string(b))
				changed := len(paths) == 2 && paths[0] != paths[1]
				if (host == wedged) != changed || len(paths) == 0 {
					t.Fatalf("%s did not match the recovery for %s: %s", host, wedged, b)
				}
			}
			if now := fixtureMaster(t, ctx, "worker"); now.generation != direct.generation {
				t.Fatalf("changed an unrelated direct connection: %v -> %v", direct, now)
			}
			if _, e = os.Stat(filepath.Join(dir, "stop")); !os.IsNotExist(e) {
				t.Fatalf("attempted to stop an SSH master: %v", e)
			}
			if wedged == "first" {
				replacement := fixtureMaster(t, ctx, "first")
				if e = first.retire(ctx); e != nil {
					t.Fatal(e)
				}
				if now := fixtureMaster(t, ctx, "first"); now.generation != replacement.generation {
					t.Fatalf("late timeout retired the replacement: %v -> %v", replacement, now)
				}
			}
		})
	}
}

func TestSSHPacketUsesReplacementMaster(t *testing.T) {
	dir := sshRecoveryFixture(t, `
initial=$(head -n 1 "$CX_SSH_FIXTURE/$host.paths")
if [ "$path" = "$initial" ]; then
  printf 'cx-wire: ready\n' >&2
  exec sleep 30
fi
echo 0
gzip -dc > "$CX_SSH_FIXTURE/received"
echo done
`)
	t.Setenv("CX_PUBLISH_RETRY_WINDOW", "8")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	fixtureMaster(t, ctx, "worker")
	packet := filepath.Join(dir, "packet")
	want := []byte("transfer payload")
	if e := os.WriteFile(packet, want, 0600); e != nil {
		t.Fatal(e)
	}
	h := endpoint{SSH: "worker", CX: "cx"}
	if e := retryTransfer(ctx, func() error {
		return sendPacket(ctx, h, []string{"snap-receive"}, packet, nil, 300*time.Millisecond)
	}); e != nil {
		t.Fatal(e)
	}
	if got, e := os.ReadFile(filepath.Join(dir, "received")); e != nil || !bytes.Equal(got, want) {
		t.Fatalf("receiver got %q, %v", got, e)
	}
}
