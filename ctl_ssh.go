package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const sshConnect = 90 * time.Second
const wireReady = "cx-wire: ready\n"
const wireProgress = "cx-wire: receiving\n"

var unsupportedMaster atomic.Bool

var detectMaster sync.Once

func checkMaster() {
	if runtime.GOOS != "windows" {
		return
	}
	detectMaster.Do(func() {
		ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		// -G expands options and exits before opening a connection.
		b, e := exec.CommandContext(ctx, "ssh", "-G", "-F", os.DevNull, "-oControlMaster=auto", "-oControlPersist=10m", "--", "cx-probe").CombinedOutput()
		if (e != nil && unsupportedDiagnostic(runtime.GOOS, string(b))) || (e == nil && !strings.Contains(strings.ToLower(string(b)), "controlmaster auto")) {
			unsupportedMaster.Store(true)
		}
	})
}
func sshOptions() ([]string, error) {
	opts := []string{"-oBatchMode=yes", "-oConnectTimeout=" + strconv.Itoa(int(sshConnect/time.Second))}
	checkMaster()
	if unsupportedMaster.Load() {
		return opts, nil
	}
	home, e := os.UserHomeDir()
	if e != nil {
		return nil, e
	}
	dir := filepath.Join(home, ".cache", "cx", "ssh")
	if len(dir)+45 >= 103 && runtime.GOOS != "windows" {
		sum := sha256.Sum256([]byte(home))
		dir = filepath.Join("/tmp", fmt.Sprintf("cx-%d-%x", os.Getuid(), sum[:6]))
	}
	if e = os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	info, e := os.Lstat(dir)
	if e != nil {
		return nil, e
	}
	if !info.IsDir() || !privateSSHDir(info) {
		return nil, fmt.Errorf("SSH control directory must be private: %s", dir)
	}
	return append(opts, "-oControlMaster=auto", "-oControlPath="+strconv.Quote(filepath.ToSlash(filepath.Join(dir, "cm-%C"))), "-oControlPersist=10m"), nil
}
func unsupportedDiagnostic(platform, diag string) bool {
	if platform != "windows" {
		return false
	}
	s := strings.ToLower(diag)
	for _, k := range []string{"controlmaster", "controlpath", "controlpersist", "unix_listener", "mux_client", "getsockname failed", "af_unix"} {
		if strings.Contains(s, k) {
			return true
		}
	}
	return false
}

type progressWriter struct {
	dst      io.Writer
	activity chan<- struct{}
	active   *atomic.Bool
	stderr   bool
	tail     string
}

func (w *progressWriter) Write(p []byte) (int, error) {
	if !w.active.Load() {
		if !w.stderr {
			w.active.Store(true)
		} else {
			part := w.tail + string(p)
			if strings.Contains(part, wireReady) {
				w.active.Store(true)
			}
			w.tail = part[max(0, len(part)-len(wireReady)):]
		}
	}
	if w.active.Load() {
		select {
		case w.activity <- struct{}{}:
		default:
		}
	}
	return w.dst.Write(p)
}

// Start the silence clock when the remote endpoint acknowledges the wire
// envelope (or emits output). The enclosing context bounds all retries.
func runActive(cmd *exec.Cmd, cancel context.CancelFunc, connect, idle time.Duration, out, diag io.Writer) error {
	activity := make(chan struct{}, 1)
	var active atomic.Bool
	cmd.Stdout = &progressWriter{dst: out, activity: activity, active: &active}
	cmd.Stderr = &progressWriter{dst: diag, activity: activity, active: &active, stderr: true}
	cmd.WaitDelay = time.Second
	if e := cmd.Start(); e != nil {
		return e
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	timer := time.NewTimer(connect)
	defer timer.Stop()
	for {
		select {
		case e := <-done:
			return e
		case <-activity:
			timer.Reset(idle)
		case <-timer.C:
			cancel()
			<-done
			return context.DeadlineExceeded
		}
	}
}
func (h endpoint) connectionBudget(idle time.Duration) time.Duration {
	if h.SSH == "" {
		return idle
	}
	d := sshConnect
	for r := h.relay; r != nil; r = r.relay {
		d += sshConnect
	}
	return d
}
func cleanDiag(b []byte) string {
	s := strings.ReplaceAll(string(b), wireReady, "")
	return strings.ReplaceAll(s, wireProgress, "")
}
func (h endpoint) rawSSH(ctx context.Context, program string, args []string, stdin string, idle time.Duration) ([]byte, error) {
	// Probe and bootstrap batches can be replayed against the same staging name.
	for i := 0; ; i++ {
		opts, e := sshOptions()
		if e != nil {
			return nil, e
		}
		c, stop := context.WithCancel(ctx)
		cmd := exec.CommandContext(c, program, append(opts, args...)...)
		cmd.Stdin = strings.NewReader(stdin)
		var b, diag bytes.Buffer
		e = runActive(cmd, stop, h.connectionBudget(idle), idle, &b, &diag)
		stop()
		if e != nil && !unsupportedMaster.Load() && unsupportedDiagnostic(runtime.GOOS, diag.String()) {
			unsupportedMaster.Store(true)
			i--
			continue
		}
		if e != nil {
			var ex *exec.ExitError
			retry := errors.Is(e, context.DeadlineExceeded) || (errors.As(e, &ex) && ex.ExitCode() == 255)
			if retry && i < 3 && ctx.Err() == nil {
				if err := pause(ctx, time.Second<<i); err != nil {
					return nil, err
				}
				continue
			}
			return nil, fmt.Errorf("%s: %w: %s", program, e, diag.String())
		}
		return b.Bytes(), nil
	}
}

type receiveProgress struct{ last time.Time }

func (w *receiveProgress) Write(p []byte) (int, error) {
	if time.Since(w.last) >= time.Second {
		if _, e := io.WriteString(os.Stderr, wireProgress); e != nil {
			return 0, e
		}
		w.last = time.Now()
	}
	return len(p), nil
}
