package main

import (
	"os"
	"syscall"
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
