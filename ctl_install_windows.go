package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Request POSIX replacement semantics so existing handles may continue
// referring to the old executable after its directory entry is replaced.
func publishBinary(from, to string) error {
	// Old images left aside by earlier upgrades are removable once their processes exit.
	if olds, _ := filepath.Glob(to + ".old.*"); len(olds) > 0 {
		for _, old := range olds {
			_ = os.Remove(old)
		}
	}
	e := replaceBinary(from, to)
	if errors.Is(e, windows.ERROR_ACCESS_DENIED) || errors.Is(e, windows.ERROR_SHARING_VIOLATION) {
		// A running image cannot be replaced, but it can be renamed: move it aside, then publish.
		aside := fmt.Sprintf("%s.old.%d", to, time.Now().UnixNano())
		if re := os.Rename(to, aside); re != nil {
			return fmt.Errorf("replace running %s: %w (rename aside: %v)", to, e, re)
		}
		return replaceBinary(from, to)
	}
	return e
}

func replaceBinary(from, to string) error {
	src, e := windows.UTF16PtrFromString(from)
	if e != nil {
		return e
	}
	h, e := windows.CreateFile(src, windows.DELETE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if e != nil {
		return e
	}
	defer windows.CloseHandle(h)
	dest, e := filepath.Abs(to)
	if e != nil {
		return e
	}
	name, e := windows.UTF16FromString(dest)
	if e != nil {
		return e
	}
	name = name[:len(name)-1]
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
	e = windows.SetFileInformationByHandle(h, windows.FileRenameInfoEx, &buffer[0], uint32(len(buffer)))
	if errors.Is(e, windows.ERROR_NOT_SUPPORTED) || errors.Is(e, windows.ERROR_INVALID_PARAMETER) {
		return replaceFile(from, to)
	}
	return e
}
