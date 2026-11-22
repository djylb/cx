package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

func transferSetting(key string, fallback time.Duration, zero bool) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	d, e := time.ParseDuration(raw + "s")
	if e != nil || d < 0 || !zero && d == 0 {
		return 0, fmt.Errorf("%s requires seconds", key)
	}
	return d, nil
}
func retryTransfer(ctx context.Context, attempt func() error) error {
	window, e := transferSetting("CX_PUBLISH_RETRY_WINDOW", 30*time.Minute, true)
	if e != nil {
		return e
	}
	end := time.Now().Add(window)
	delay := 5 * time.Second
	for {
		e = attempt()
		if e == nil || ctx.Err() != nil {
			return e
		}
		var ex *exec.ExitError
		connection := errors.Is(e, ctlExit(9)) || errors.Is(e, context.DeadlineExceeded) || errors.Is(e, io.ErrUnexpectedEOF) || errors.Is(e, io.ErrClosedPipe) || errors.Is(e, syscall.EPIPE) || errors.As(e, &ex) && (ex.ExitCode() == 255 || ex.ExitCode() == 9)
		if !connection || time.Now().Add(delay).After(end) {
			return e
		}
		diagnosticf("cx: transfer interrupted; retry in %s: %v\n", delay, e)
		if err := pause(ctx, delay); err != nil {
			return err
		}
		delay = min(2*delay, time.Minute)
	}
}
func sendTask(ctx context.Context, h endpoint, req jobSubmission, raw []byte, progress func(int64)) error {
	idle, e := transferSetting("CX_TRANSFER_IDLE", 120*time.Second, false)
	if e != nil {
		return e
	}
	d, e := submissionDir(req.Host, req.ID)
	if e != nil {
		return e
	}
	digest := snapSum(raw)
	p := filepath.Join(d, "packet-"+digest)
	if e = snapImmutable(p, raw); e != nil {
		return e
	}
	return retryTransfer(ctx, func() error {
		return sendPacket(ctx, h, []string{"task-receive", req.ID, digest, strconv.Itoa(len(raw)), idle.String()}, p, progress, idle)
	})
}
func taskPacketPath(root, id string) (string, error) {
	dir, e := jobDir(filepath.Join(root, ".work"), id)
	return filepath.Join(dir, ".offered.json"), e
}
func receiveTask(ctx context.Context, a []string) error {
	if len(a) != 4 || !snapHex(a[1], 64) {
		return errors.New("invalid task offer")
	}
	size, e := strconv.ParseInt(a[2], 10, 64)
	if e != nil || size < 0 {
		return errors.New("invalid task size")
	}
	idle, e := time.ParseDuration(a[3])
	if e != nil || idle <= 0 {
		return errors.New("invalid transfer timeout")
	}
	root, e := stateRoot()
	if e != nil {
		return e
	}
	p, e := taskPacketPath(root, a[0])
	if e != nil {
		return e
	}
	lock, e := jobOperationLock(ctx, root, a[0])
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	if hash, hashErr := fileHash(p); hashErr == nil {
		if hash != a[1] {
			return errors.New("job already bound to another request")
		}
		_, hashErr = fmt.Fprintln(os.Stdout, "done")
		return hashErr
	} else if !os.IsNotExist(hashErr) {
		return hashErr
	}
	if e = os.MkdirAll(filepath.Dir(p), 0700); e != nil {
		return e
	}
	part := filepath.Join(filepath.Dir(p), "."+a[1]+".part")
	in := interruptibleStdin()
	stop := context.AfterFunc(ctx, func() { _ = in.Close() })
	defer stop()
	timer := time.AfterFunc(idle, func() { _ = in.Close() })
	defer timer.Stop()
	if e = receivePacket(part, a[1], size, snapActiveReader{in, timer, idle}, os.Stdout); e != nil {
		return e
	}
	return os.Rename(part, p)
}
func startInstalledTask(ctx context.Context, id, hash string) error {
	root, e := stateRoot()
	if e != nil {
		return e
	}
	p, e := taskPacketPath(root, id)
	if e != nil {
		return e
	}
	b, e := readStateFile(p)
	if e != nil {
		return e
	}
	if snapSum(b) != hash {
		return errors.New("task packet checksum mismatch")
	}
	return startNodeJob(ctx, string(b))
}

func transferLock(ctx context.Context, path string) (*os.File, error) {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Hour)
	defer cancel()
	for {
		lock, e := lockPath(path, os.O_CREATE)
		if e != nil || lock != nil {
			return lock, e
		}
		if e = pause(ctx, 150*time.Millisecond); e != nil {
			return nil, e
		}
	}
}
