//go:build darwin || linux

package main

import (
	"context"
	"io"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// The command is the sole stdin reader. On Darwin select reports named FIFO
// hangup as readable EOF, which poll and kqueue can miss.
func readStdin(ctx context.Context, p []byte) (int, error) {
	for {
		if e := ctx.Err(); e != nil {
			return 0, e
		}
		wait := 50 * time.Millisecond
		if deadline, ok := ctx.Deadline(); ok {
			wait = min(wait, time.Until(deadline))
			if wait <= 0 {
				return 0, context.DeadlineExceeded
			}
		}
		var fds unix.FdSet
		fds.Set(unix.Stdin)
		timeout := unix.NsecToTimeval(wait.Nanoseconds())
		ready, e := unix.Select(unix.Stdin+1, &fds, nil, nil, &timeout)
		if e == unix.EINTR || (e == nil && ready == 0) {
			continue
		}
		if e != nil {
			return 0, os.NewSyscallError("select stdin", e)
		}
		// If readiness wins, return the data even if cancellation races
		// the read. The next read checks its own deadline.
		n, e := unix.Read(unix.Stdin, p)
		switch e {
		case unix.EINTR, unix.EAGAIN:
			continue
		case nil:
			if n == 0 {
				return 0, io.EOF
			}
			return n, nil
		default:
			return 0, os.NewSyscallError("read stdin", e)
		}
	}
}
