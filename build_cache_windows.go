package main

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

func cacheDirectory(info os.FileInfo) bool {
	return info.IsDir() && info.Sys().(*syscall.Win32FileAttributeData).FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT == 0
}

func cacheMountID(f *os.File) (uint64, error) {
	var info windows.ByHandleFileInformation
	err := windows.GetFileInformationByHandle(windows.Handle(f.Fd()), &info)
	return uint64(info.VolumeSerialNumber), err
}

func cacheProcessGroup(*exec.Cmd) int { return 0 }

func cacheRunAlive(run string, _ []int) bool {
	job, err := openTrackedJob(run)
	if err == nil {
		var counts struct {
			Times      [4]int64
			PageFaults uint32
			Processes  uint32
			Active     uint32
			Terminated uint32
		}
		err = windows.QueryInformationJobObject(job, windows.JobObjectBasicAccountingInformation,
			uintptr(unsafe.Pointer(&counts)), uint32(unsafe.Sizeof(counts)), nil)
		windows.CloseHandle(job)
		if err != nil || counts.Active != 0 {
			return true
		}
	} else if !errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
		return true
	}

	// Closing the last handle removes the job's name even while members
	// survive. A resumed runner can also create a new object with that name.
	view := inventory{}
	for _, p := range scanProcesses(run, &view) {
		if p.Marked {
			return true
		}
	}
	return view.scanErr != nil
}

// LockFileEx locks belong to the locking process. The named job object
// and run marker extend cache protection to surviving descendants.
func inheritCacheLeases(*exec.Cmd, []*os.File) {}
