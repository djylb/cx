package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Request POSIX replacement semantics so existing handles may continue
// referring to the old executable after its directory entry is replaced.
func publishBinary(ctx context.Context, from, to string) error {
	to, err := filepath.Abs(to)
	if err != nil {
		return err
	}
	// Serialize publishers and cleanup for this destination across controllers.
	lock, err := lockUntil(ctx, to+".install.lock")
	if err != nil {
		return err
	}
	defer closeBestEffort(lock)
	err = publishWithRollback(from, to, replaceBinary, moveBinary, isWindowsFileConflict)
	if err != nil {
		return err
	}
	// Once publication succeeds, images left aside by earlier upgrades can be
	// removed as their processes exit. Keep a failed rollback's image intact.
	dir, prefix := filepath.Dir(to), filepath.Base(to)+".old."
	entries, _ := os.ReadDir(dir)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) {
			_ = os.Remove(filepath.Join(dir, entry.Name()))
		}
	}
	return nil
}

func moveBinary(from, to string) error {
	src, err := windows.UTF16PtrFromString(extendedWindowsPath(from))
	if err != nil {
		return err
	}
	dst, err := windows.UTF16PtrFromString(extendedWindowsPath(to))
	if err != nil {
		return err
	}
	// MoveFile fails if the target exists. In particular, rollback cannot
	// overwrite a target installed by software outside cx's install lock.
	return windows.MoveFile(src, dst)
}

func replaceBinary(from, to string) error {
	err := renameBinaryPOSIX(from, to)
	if posixRenameUnavailable(err) {
		return retryWindowsFileIO(2*time.Second, isWindowsFileConflict, func() error {
			return os.Rename(from, to)
		})
	}
	return err
}

// Volumes or filter drivers that reject the POSIX rename request itself; MoveFileEx still works there.
func posixRenameUnavailable(err error) bool {
	return errors.Is(err, windows.ERROR_NOT_SUPPORTED) || errors.Is(err, windows.ERROR_INVALID_PARAMETER) ||
		errors.Is(err, windows.ERROR_INVALID_NAME) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND)
}

func openBinaryForReplace(path string, access uint32) (windows.Handle, error) {
	if path == "" {
		return windows.InvalidHandle, windows.ERROR_FILE_NOT_FOUND
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return windows.InvalidHandle, err
	}
	if len(path) >= 248 {
		path = extendedWindowsPath(path)
	}
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return windows.InvalidHandle, err
	}
	return windows.CreateFile(name, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
}

func isWindowsFileConflict(err error) bool {
	return errors.Is(err, windows.ERROR_SHARING_VIOLATION) || errors.Is(err, windows.ERROR_ACCESS_DENIED)
}

func retryWindowsFileIO(budget time.Duration, retryable func(error) bool, attempt func() error) error {
	deadline := time.Now().Add(budget)
	delay := 5 * time.Millisecond
	for {
		err := attempt()
		if err == nil || !retryable(err) {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return err
		}
		time.Sleep(min(delay, remaining))
		delay = min(2*delay, 40*time.Millisecond)
	}
}

// POSIX replacement lets open readers finish reading the previous contents.
func renameBinaryPOSIX(from, to string) error {
	h, e := openBinaryForReplace(from, windows.DELETE)
	if e != nil {
		return e
	}
	defer func() { _ = windows.CloseHandle(h) }()
	dest, e := filepath.Abs(to)
	if e != nil {
		return e
	}
	name, e := windows.UTF16FromString(dest)
	if e != nil {
		return e
	}
	name = name[:len(name)-1]
	// Field order is fixed by the Windows FILE_RENAME_INFO ABI.
	type renameInfo struct {
		Flags  uint32
		Root   windows.Handle
		Length uint32
		Name   uint16
	}
	offset := int(unsafe.Offsetof(renameInfo{}.Name))
	buffer := make([]byte, offset+len(name)*2)
	record := (*renameInfo)(unsafe.Pointer(&buffer[0]))
	record.Flags = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	record.Length = uint32(len(name) * 2)
	copy(unsafe.Slice((*uint16)(unsafe.Pointer(&buffer[offset])), len(name)), name)
	return windows.SetFileInformationByHandle(h, windows.FileRenameInfoEx, &buffer[0], uint32(len(buffer)))
}
