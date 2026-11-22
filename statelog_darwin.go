package main

import (
	"os"

	"golang.org/x/sys/unix"
)

func syncJobFile(f *os.File) error {
	if _, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0); err != nil {
		return &os.PathError{Op: "fullfsync", Path: f.Name(), Err: err}
	}
	return nil
}
