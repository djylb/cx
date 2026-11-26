package main

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func cleanupError(err error, root string) error {
	if !errors.Is(err, windows.ERROR_ACCESS_DENIED) && !errors.Is(err, windows.ERROR_SHARING_VIOLATION) &&
		!errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return err
	}
	names := processNames()
	deadline := time.Now().Add(3 * time.Second)
	var holders []error
	var inspectErr error
	walkErr := filepath.WalkDir(root, func(path string, _ fs.DirEntry, visitErr error) error {
		if time.Now().After(deadline) {
			return errors.New("open-handle scan exceeded deadline")
		}
		if visitErr != nil {
			if inspectErr == nil && !errors.Is(visitErr, fs.ErrNotExist) {
				inspectErr = visitErr
			}
			return nil
		}
		pids, queryErr := fileProcessIDs(path)
		if queryErr != nil {
			if inspectErr == nil && !errors.Is(queryErr, fs.ErrNotExist) {
				inspectErr = fmt.Errorf("inspect %s: %w", path, queryErr)
			}
			return nil
		}
		for _, pid := range pids {
			if name := names[pid]; name != "" {
				holders = append(holders, fmt.Errorf("%s: open in PID %d (%s)", path, pid, name))
			} else {
				holders = append(holders, fmt.Errorf("%s: open in PID %d", path, pid))
			}
		}
		return nil
	})
	if walkErr == nil && inspectErr == nil && len(holders) == 0 {
		walkErr = errors.New("no open handles reported under " + root)
	}
	return errors.Join(err, errors.Join(holders...), walkErr, inspectErr)
}

func fileProcessIDs(path string) ([]uintptr, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	name, err := windows.UTF16PtrFromString(extendedWindowsPath(abs))
	if err != nil {
		return nil, err
	}
	// Attribute-only opens do not appear in the returned process list.
	h, err := windows.CreateFile(name, windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = windows.CloseHandle(h) }()
	const fileProcessIDsUsingFileInformation = 47
	width := uint32(unsafe.Sizeof(uintptr(0)))
	buf := make([]uintptr, 16)
	var info windows.IO_STATUS_BLOCK
	for {
		err = windows.NtQueryInformationFile(h, &info, (*byte)(unsafe.Pointer(&buf[0])),
			uint32(len(buf))*width, fileProcessIDsUsingFileInformation)
		switch {
		case err == nil:
			// A ULONG count is followed by a pointer-aligned array of PIDs.
			n := uint32(buf[0])
			if n >= uint32(len(buf)) {
				return nil, windows.ERROR_INVALID_DATA
			}
			return buf[1 : n+1], nil
		case errors.Is(err, windows.STATUS_INFO_LENGTH_MISMATCH), errors.Is(err, windows.STATUS_BUFFER_OVERFLOW),
			errors.Is(err, windows.STATUS_BUFFER_TOO_SMALL):
			if uint64(len(buf))*uint64(width)*2 > uint64(^uint32(0)) {
				return nil, err
			}
			buf = make([]uintptr, len(buf)*2)
		default:
			return nil, err
		}
	}
}

func processNames() map[uintptr]string {
	names := map[uintptr]string{}
	snap, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		return names
	}
	defer func() { _ = windows.CloseHandle(snap) }()
	var e windows.ProcessEntry32
	e.Size = uint32(unsafe.Sizeof(e))
	for err = windows.Process32First(snap, &e); err == nil; err = windows.Process32Next(snap, &e) {
		names[uintptr(e.ProcessID)] = windows.UTF16ToString(e.ExeFile[:])
	}
	return names
}
