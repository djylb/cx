//go:build darwin || linux

package main

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func openJobFile(path string, mode jobFileMode) (*os.File, error) {
	var flags int
	switch mode {
	case jobFileRead, jobFileDirectory, jobFileDirectoryID, jobFileSync:
		flags = os.O_RDONLY
	case jobFileNew:
		flags = os.O_WRONLY | os.O_CREATE | os.O_EXCL | os.O_APPEND
	case jobFileAppend:
		flags = os.O_WRONLY | os.O_APPEND
	case jobFileAppendCreate:
		flags = os.O_WRONLY | os.O_APPEND | os.O_CREATE
	case jobFileLockExisting:
		flags = os.O_RDWR
	case jobFileLock:
		flags = os.O_RDWR | os.O_CREATE
	}
	return os.OpenFile(path, flags, 0600)
}

func removeJobSegment(path string) { _ = os.Remove(path) }

func stateHistorySharingConflict(_ error) bool { return false }

func syncJobDir(path string) error {
	f, err := openJobFile(path, jobFileDirectory)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func jobDirectoryKey(path string) (string, error) {
	f, err := openJobFile(path, jobFileDirectoryID)
	if err != nil {
		return "", err
	}
	defer closeBestEffort(f)
	return jobDirectoryFileKey(f)
}

func jobDirectoryFileKey(f *os.File) (string, error) {
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	stat := info.Sys().(*syscall.Stat_t)
	return fmt.Sprintf("%x:%x", stat.Dev, stat.Ino), nil
}
