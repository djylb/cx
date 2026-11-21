package main

import (
	"errors"
	"os"
	"syscall"
	"time"
	"unsafe"
)

var lockFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx")

func tryLock(f *os.File) (bool, error) {
	return tryLockMode(f, 2) // LOCKFILE_EXCLUSIVE_LOCK
}

func trySharedLock(f *os.File) (bool, error) {
	return tryLockMode(f, 0)
}

func tryLockMode(f *os.File, mode uintptr) (bool, error) {
	var offset syscall.Overlapped
	// LOCKFILE_FAIL_IMMEDIATELY. Closing the handle releases the lock.
	ok, _, err := lockFileEx.Call(f.Fd(), mode|1, 0, 1, 0, uintptr(unsafe.Pointer(&offset)))
	if ok != 0 {
		return true, nil
	}
	if err == syscall.Errno(33) {
		return false, nil
	} // ERROR_LOCK_VIOLATION
	return false, err
}

// Windows readers can briefly hold handles without FILE_SHARE_DELETE.
func replaceFile(from, to string) error {
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := os.Rename(from, to)
		if !(errors.Is(err, syscall.Errno(32)) || errors.Is(err, syscall.ERROR_ACCESS_DENIED)) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}
