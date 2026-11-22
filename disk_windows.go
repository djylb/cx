package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/windows"
)

func cacheAccessTime(info os.FileInfo) time.Time {
	stamp := info.Sys().(*syscall.Win32FileAttributeData).LastAccessTime
	return time.Unix(0, stamp.Nanoseconds())
}

func diskSpace(path string) (free, total uint64, err error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, 0, err
	}
	err = windows.GetDiskFreeSpaceEx(p, &free, &total, nil)
	return free, total, err
}

// Resolve junctions and mapped drives before deciding whether a job can use
// the volume's local locking and file sharing semantics.
func stateDiskLocal(path string) (bool, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false, err
	}
	name, err := windows.UTF16PtrFromString(extendedWindowsPath(resolved))
	if err != nil {
		return false, err
	}
	var volume [windows.MAX_PATH]uint16
	if err = windows.GetVolumePathName(name, &volume[0], uint32(len(volume))); err != nil {
		return false, err
	}
	switch windows.GetDriveType(&volume[0]) {
	case windows.DRIVE_FIXED, windows.DRIVE_REMOVABLE, windows.DRIVE_RAMDISK:
		return true, nil
	case windows.DRIVE_REMOTE:
		return false, nil
	default:
		return false, fmt.Errorf("cannot confirm local state volume at %s", resolved)
	}
}

func markStateNotIndexed(path string) error {
	name, err := windows.UTF16PtrFromString(extendedWindowsPath(path))
	if err != nil {
		return err
	}
	attr, err := windows.GetFileAttributes(name)
	if err != nil || attr&windows.FILE_ATTRIBUTE_NOT_CONTENT_INDEXED != 0 {
		return err
	}
	return windows.SetFileAttributes(name, (attr&^windows.FILE_ATTRIBUTE_NORMAL)|windows.FILE_ATTRIBUTE_NOT_CONTENT_INDEXED)
}
