package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"

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
	err := renameFilePOSIX(from, to)
	if posixRenameUnavailable(err) {
		return retryWindowsFileIO(stateReplaceBudget, isWindowsFileConflict, func() error {
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
