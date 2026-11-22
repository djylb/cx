package main

import (
	"context"
	"errors"
	"io"
	"os"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var peekNamedPipe = windows.NewLazySystemDLL("kernel32.dll").NewProc("PeekNamedPipe")

func stdinPipeAvailable(handle windows.Handle) (uint32, error) {
	var available uint32
	ok, _, err := peekNamedPipe.Call(uintptr(handle), 0, 0, 0, uintptr(unsafe.Pointer(&available)), 0)
	if ok == 0 {
		return 0, os.NewSyscallError("PeekNamedPipe", err)
	}
	return available, nil
}

func stdinPipeResult(n int, err error) (int, error) {
	switch {
	case errors.Is(err, windows.ERROR_BROKEN_PIPE),
		errors.Is(err, windows.ERROR_NO_DATA),
		errors.Is(err, windows.ERROR_PIPE_NOT_CONNECTED):
		return n, io.EOF
	default:
		return n, err
	}
}

func readStdin(ctx context.Context, p []byte) (int, error) {
	if e := ctx.Err(); e != nil {
		return 0, e
	}
	if ctx.Done() == nil {
		return os.Stdin.Read(p)
	}
	handle := windows.Handle(os.Stdin.Fd())
	kind, err := windows.GetFileType(handle)
	if err != nil {
		return 0, os.NewSyscallError("GetFileType stdin", err)
	}
	if kind == windows.FILE_TYPE_PIPE {
		// SSH can retain a write handle after its sender disconnects.
		// An idle deadline still applies while the pipe has no data.
		return stdinPipeResult(readReadyPipe(ctx, p, func() (uint32, error) {
			return stdinPipeAvailable(handle)
		}, os.Stdin.Read))
	}
	readDone := make(chan struct{})
	cancelDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(cancelDone)
		// Retry if cancellation races the console read's submission.
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-readDone:
				return
			default:
			}
			_ = windows.CancelIoEx(handle, nil)
			select {
			case <-readDone:
				return
			case <-tick.C:
			}
		}
	})
	n, e := os.Stdin.Read(p)
	close(readDone)
	if !stop() {
		<-cancelDone
	}
	// An interrupted ReadConsole may report a successful zero-byte read.
	if cause := ctx.Err(); n == 0 && cause != nil {
		return 0, cause
	}
	return n, e
}
