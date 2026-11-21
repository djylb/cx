//go:build darwin || linux

package main

import (
	"errors"
	"os"
	"syscall"
)

func tryLock(f *os.File) (bool, error) {
	return tryFlock(f, syscall.LOCK_EX)
}

func trySharedLock(f *os.File) (bool, error) {
	return tryFlock(f, syscall.LOCK_SH)
}

func tryFlock(f *os.File, mode int) (bool, error) {
	err := syscall.Flock(int(f.Fd()), mode|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		return false, nil
	}
	return err == nil, err
}

func replaceFile(from, to string) error { return os.Rename(from, to) }
