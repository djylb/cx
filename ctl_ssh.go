package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/djylb/timeshim"
)

const sshConnect = 90 * time.Second
const wireReady = "cx-wire: ready\n"
const wireProgress = "cx-wire: receiving\n"

var unsupportedMaster atomic.Bool

var detectMaster sync.Once
var masterChecked = make(chan struct{})

func checkMaster() {
	_ = checkMasterWithin(context.Background())
}

func checkMasterWithin(ctx context.Context) error {
	if runtime.GOOS != "windows" {
		return nil
	}
	detectMaster.Do(func() {
		go func() {
			defer close(masterChecked)
			probe, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			// -G expands options and exits before opening a connection.
			b, e := exec.CommandContext(probe, "ssh", "-G", "-F", os.DevNull, "-oControlMaster=auto", "-oControlPersist=10m", "--", "cx-probe").CombinedOutput()
			if (e != nil && unsupportedDiagnostic(runtime.GOOS, string(b))) || (e == nil && !strings.Contains(strings.ToLower(string(b)), "controlmaster auto")) {
				unsupportedMaster.Store(true)
			}
		}()
	})
	select {
	case <-masterChecked:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func sshOptions() ([]string, error) {
	opts, _, e := endpointSSHOptions(context.Background(), "")
	return opts, e
}

// Each alias on this machine has a current control socket generation. Changed
// generations leave existing sessions alone, including masters that cannot
// respond to control commands.
type sshMaster struct {
	state      string
	generation string
}

const sshGenerationBytes = 8

func endpointSSHOptions(ctx context.Context, host string) ([]string, sshMaster, error) {
	opts := []string{"-oBatchMode=yes", "-oConnectTimeout=" + strconv.Itoa(int(sshConnect/time.Second))}
	var master sshMaster
	if e := ctx.Err(); e != nil {
		return nil, master, e
	}
	if e := checkMasterWithin(ctx); e != nil {
		return nil, master, e
	}
	if unsupportedMaster.Load() {
		return opts, master, nil
	}
	home, e := os.UserHomeDir()
	if e != nil {
		return nil, master, e
	}
	dir := filepath.Join(home, ".cache", "cx", "ssh")
	// Allow room for the expanded %C, the generation and OpenSSH's temporary
	// listener suffix, even when OpenSSH creates a new master.
	if len(dir)+len("/cm-")+40+1+2*sshGenerationBytes+17 >= 104 && runtime.GOOS != "windows" {
		sum := sha256.Sum256([]byte(home))
		dir = filepath.Join("/tmp", fmt.Sprintf("cx-%d-%x", os.Getuid(), sum[:6]))
	}
	if e = os.MkdirAll(dir, 0700); e != nil {
		return nil, master, e
	}
	info, e := os.Lstat(dir)
	if e != nil {
		return nil, master, e
	}
	if !info.IsDir() || !privateSSHDir(info) {
		return nil, master, fmt.Errorf("SSH control directory must be private: %s", dir)
	}
	socket := "cm-%C"
	if host != "" {
		sum := sha256.Sum256([]byte(host))
		master.state = filepath.Join(dir, fmt.Sprintf("master-%x", sum[:sshGenerationBytes]))
		lock, lockErr := lockUntil(ctx, master.state+".lock")
		if lockErr != nil {
			return nil, master, lockErr
		}
		defer closeBestEffort(lock)
		master.generation, e = master.current()
		if e != nil {
			return nil, master, e
		}
		socket += "-" + master.generation
	}
	opts = append(opts, "-oControlMaster=auto", "-oControlPath="+strconv.Quote(filepath.ToSlash(filepath.Join(dir, socket))), "-oControlPersist=10m")
	return opts, master, nil
}

// The generation file is private and read and written under its lock. If a
// controller dies during a write, a fresh generation repairs the short file.
func (m sshMaster) current() (string, error) {
	b, e := os.ReadFile(m.state)
	if os.IsNotExist(e) {
		return strings.TrimPrefix(filepath.Base(m.state), "master-"), nil
	}
	if e != nil {
		return "", e
	}
	if gen, err := hex.DecodeString(string(b)); err == nil && len(gen) == sshGenerationBytes {
		return string(b), nil
	}
	return m.next()
}

func (m sshMaster) next() (string, error) {
	var nonce [sshGenerationBytes]byte
	_, _ = rand.Read(nonce[:])
	gen := hex.EncodeToString(nonce[:])
	return gen, os.WriteFile(m.state, []byte(gen), 0600)
}

func (m sshMaster) retire(ctx context.Context) error {
	lock, e := lockUntil(ctx, m.state+".lock")
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	if e = ctx.Err(); e != nil {
		return e
	}
	gen, e := m.current()
	if e == nil && gen == m.generation {
		_, e = m.next()
	}
	return e
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
	tail     string
	stderr   bool
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

var errWireSilence = fmt.Errorf("wire silence: %w", context.DeadlineExceeded)
var errSSHMasterUnsupported = fmt.Errorf("%w: SSH multiplexing unsupported", ctlExit(9))

func runActive(cmd *exec.Cmd, cancel context.CancelFunc, connect, idle time.Duration, out, diag io.Writer) error {
	_, e := runWithActivity(context.Background(), cmd, cancel, connect, idle, out, diag)
	return e
}

// Acknowledgement or output switches from the connection clock to the
// silence clock. Only those clocks can produce errWireSilence.
func runWithActivity(ctx context.Context, cmd *exec.Cmd, cancel context.CancelFunc, connect, idle time.Duration, out, diag io.Writer) (ready bool, err error) {
	activity := make(chan struct{}, 1)
	var active atomic.Bool
	cmd.Stdout = &progressWriter{dst: out, activity: activity, active: &active}
	cmd.Stderr = &progressWriter{dst: diag, activity: activity, active: &active, stderr: true}
	cmd.WaitDelay = time.Second
	if e := cmd.Start(); e != nil {
		return false, e
	}
	done := make(chan processWait, 1)
	go func() { done <- processWait{cmd.Wait()} }()
	timer := timeshim.NewTimer(timeshim.Or(contextClock(ctx)), connect)
	defer timer.Stop()
	for {
		select {
		case waited := <-done:
			if e := ctx.Err(); e != nil {
				return active.Load(), e
			}
			return active.Load(), waited.err
		case <-ctx.Done():
			cancel()
			<-done
			return active.Load(), ctx.Err()
		case <-activity:
			timer.Reset(idle)
		case <-timer.C:
			select {
			case waited := <-done:
				return active.Load(), waited.err
			case <-activity:
				timer.Reset(idle)
				continue
			default:
			}
			cause := ctx.Err()
			cancel()
			<-done
			if cause != nil {
				return active.Load(), cause
			}
			return active.Load(), errWireSilence
		}
	}
}

type transportProcess struct {
	*exec.Cmd
	ctx           context.Context
	master        sshMaster
	connect, idle time.Duration
}

func (p *transportProcess) run(ctx context.Context, cancel context.CancelFunc, out io.Writer, diag *bytes.Buffer) error {
	ready, e := runWithActivity(p.ctx, p.Cmd, cancel, p.connect, p.idle, out, diag)
	if cause := ctx.Err(); cause != nil {
		return cause
	}
	if e == nil || p.master.state == "" {
		return e
	}
	if !ready && unsupportedDiagnostic(runtime.GOOS, diag.String()) {
		unsupportedMaster.Store(true)
		return errSSHMasterUnsupported
	}
	var ex *exec.ExitError
	if errors.Is(e, errWireSilence) || !ready && errors.As(e, &ex) && ex.ExitCode() == 255 {
		if retireErr := p.master.retire(ctx); retireErr != nil && ctx.Err() == nil {
			diagnosticf("cx: SSH control socket: %v\n", retireErr)
		}
	}
	if cause := ctx.Err(); cause != nil {
		return cause
	}
	return e
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
		opts, master, e := endpointSSHOptions(ctx, h.SSH)
		if e != nil {
			return nil, e
		}
		c, stop := context.WithCancel(ctx)
		cmd := transportProcess{exec.CommandContext(c, program, append(opts, args...)...), c, master, sshConnect, idle}
		cmd.Stdin = strings.NewReader(stdin)
		var b, diag bytes.Buffer
		e = cmd.run(ctx, stop, &b, &diag)
		stop()
		if errors.Is(e, errSSHMasterUnsupported) {
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
