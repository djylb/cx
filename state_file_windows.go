package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	stateReadBudget    = 40 * time.Millisecond
	stateReplaceBudget = 2 * time.Second
)

func readStateFile(path string) ([]byte, error) {
	f, err := openShared(path)
	if err != nil {
		return nil, err
	}
	defer closeBestEffort(f)
	return io.ReadAll(f)
}

func openShared(path string) (*os.File, error) {
	var h windows.Handle
	err := retryWindowsFileIO(stateReadBudget, isWindowsOpenTransient, func() error {
		var openErr error
		h, openErr = openWindowsFile(path, windows.GENERIC_READ)
		return openErr
	})
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}

func openWindowsFile(path string, access uint32) (windows.Handle, error) {
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

func isWindowsOpenTransient(err error) bool {
	return os.IsNotExist(err) || isWindowsFileConflict(err)
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

func replaceFile(from, to string) error {
	return retryWindowsFileIO(stateReplaceBudget, isWindowsFileConflict, func() error {
		// POSIX replacement is preferred; when a volume or filter driver rejects it for
		// any reason, MoveFileEx decides. A real failure surfaces from MoveFileEx too.
		if renameFilePOSIX(from, to) != nil {
			return os.Rename(from, to)
		}
		return nil
	})
}

func extendedWindowsPath(path string) string {
	switch {
	case strings.HasPrefix(path, `\\?\`), strings.HasPrefix(path, `\\.\`):
		return path
	case strings.HasPrefix(path, `\\`):
		return `\\?\UNC\` + path[2:]
	default:
		return `\\?\` + path
	}
}

// POSIX replacement lets open readers finish reading the previous contents.
func renameFilePOSIX(from, to string) error {
	h, e := openWindowsFile(from, windows.DELETE)
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
