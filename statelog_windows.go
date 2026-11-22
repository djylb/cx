package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

const jobFileDelete jobFileMode = jobFileSync + 1

func openJobFile(path string, mode jobFileMode) (*os.File, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	name, err := windows.UTF16PtrFromString(extendedWindowsPath(abs))
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	var access, disposition uint32
	attributes := uint32(windows.FILE_ATTRIBUTE_NORMAL)
	switch mode {
	case jobFileRead:
		access, disposition = windows.GENERIC_READ, windows.OPEN_EXISTING
	case jobFileNew:
		// FlushFileBuffers requires GENERIC_WRITE. On a newly created
		// segment, the sole writer advances the synchronous handle's
		// offset using Write only, so every write is an append.
		access, disposition = windows.GENERIC_WRITE, windows.CREATE_NEW
	case jobFileAppend:
		access, disposition = windows.FILE_APPEND_DATA|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE, windows.OPEN_EXISTING
	case jobFileAppendCreate:
		access, disposition = windows.FILE_APPEND_DATA|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE, windows.OPEN_ALWAYS
	case jobFileDirectoryID:
		access, disposition = windows.FILE_READ_ATTRIBUTES, windows.OPEN_EXISTING
		attributes = windows.FILE_FLAG_BACKUP_SEMANTICS
	case jobFileDirectory:
		access, disposition = windows.GENERIC_READ|windows.GENERIC_WRITE, windows.OPEN_EXISTING
		attributes = windows.FILE_FLAG_BACKUP_SEMANTICS
	case jobFileLockExisting, jobFileSync:
		access, disposition = windows.GENERIC_READ|windows.GENERIC_WRITE, windows.OPEN_EXISTING
	case jobFileLock:
		access, disposition = windows.GENERIC_READ|windows.GENERIC_WRITE, windows.OPEN_ALWAYS
	case jobFileDelete:
		access, disposition = windows.DELETE, windows.OPEN_EXISTING
	}
	h, err := windows.CreateFile(name, access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, disposition, attributes, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}

func syncJobFile(f *os.File) error { return f.Sync() }

func stateHistorySharingConflict(err error) bool {
	return errors.Is(err, windows.ERROR_SHARING_VIOLATION)
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
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(f.Fd()), &info); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x:%08x%08x", info.VolumeSerialNumber, info.FileIndexHigh, info.FileIndexLow), nil
}

func syncJobDir(path string) error {
	f, err := openJobFile(path, jobFileDirectory)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func removeJobSegment(path string) {
	retireWindowsSegment(path, posixDeleteSegment)
}

func posixDeleteSegment(f *os.File) error {
	flags := uint32(windows.FILE_DISPOSITION_DELETE | windows.FILE_DISPOSITION_POSIX_SEMANTICS |
		windows.FILE_DISPOSITION_IGNORE_READONLY_ATTRIBUTE)
	return windows.SetFileInformationByHandle(windows.Handle(f.Fd()), windows.FileDispositionInfoEx,
		(*byte)(unsafe.Pointer(&flags)), uint32(unsafe.Sizeof(flags)))
}

func retireWindowsSegment(path string, remove func(*os.File) error) {
	if f, err := openJobFile(path, jobFileDelete); err == nil {
		// A legacy delete can leave the name delete-pending while readers hold it.
		_ = remove(f)
		_ = f.Close()
	}
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
