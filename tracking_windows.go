package main

import (
	"encoding/binary"
	"errors"
	"os"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

func jobObjectName(id string) string { return "Local\\cxrun-" + id }
func openTrackedJob(id string) (windows.Handle, error) {
	name, err := windows.UTF16PtrFromString(jobObjectName(id))
	if err != nil {
		return 0, err
	}
	h, _, e := kernel.NewProc("OpenJobObjectW").Call(4, 0, uintptr(unsafe.Pointer(name)))
	if h == 0 {
		return 0, e
	}
	return windows.Handle(h), nil
}
func handleStart(h windows.Handle) uint64 {
	var c, e, k, u windows.Filetime
	if windows.GetProcessTimes(h, &c, &e, &k, &u) != nil {
		return 0
	}
	return uint64(c.HighDateTime)<<32 | uint64(c.LowDateTime)
}
func processStart(pid int) uint64 {
	h, e := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(pid))
	if e != nil {
		return 0
	}
	defer windows.CloseHandle(h)
	return handleStart(h)
}
func inJob(h, job windows.Handle) bool {
	var member int32
	ok, _, _ := kernel.NewProc("IsProcessInJob").Call(uintptr(h), uintptr(job), uintptr(unsafe.Pointer(&member)))
	return ok != 0 && member != 0
}

// Read only the environment referenced by the target's process parameters.
// A 64-bit runner can also inspect a WOW64 process through its 32-bit PEB.
func hasRun(h windows.Handle, id string) (bool, error) {
	var info windows.PROCESS_BASIC_INFORMATION
	err := windows.NtQueryInformationProcess(h, windows.ProcessBasicInformation, unsafe.Pointer(&info), uint32(unsafe.Sizeof(info)), nil)
	if err != nil {
		return false, err
	}
	peb := uintptr(unsafe.Pointer(info.PebBaseAddress))
	word := unsafe.Sizeof(peb)
	paramsOff := unsafe.Offsetof(windows.PEB{}.ProcessParameters)
	envOff := unsafe.Offsetof(windows.RTL_USER_PROCESS_PARAMETERS{}.Environment)
	if word == 8 {
		var wow uintptr
		if err = windows.NtQueryInformationProcess(h, windows.ProcessWow64Information, unsafe.Pointer(&wow), uint32(word), nil); err != nil {
			return false, err
		}
		if wow != 0 {
			peb, word, paramsOff, envOff = wow, 4, 0x10, 0x48
		}
	}
	readPtr := func(addr uintptr) (uintptr, error) {
		var ptr uintptr
		err := windows.ReadProcessMemory(h, addr, (*byte)(unsafe.Pointer(&ptr)), word, nil)
		return ptr, err
	}
	params, err := readPtr(peb + paramsOff)
	if err != nil {
		return false, err
	}
	env, err := readPtr(params + envOff)
	if err != nil {
		return false, err
	}
	want, _ := windows.UTF16FromString("CXRUN_RUN=" + id)
	// Read up to each page boundary. Match whole UTF-16 entries without
	// retaining or printing any other process environment values.
	var page [4096]byte
	pos := 0
	for env != 0 {
		n := uintptr(len(page)) - env%uintptr(len(page))
		if n%2 != 0 {
			return false, windows.ERROR_INVALID_DATA
		}
		if err = windows.ReadProcessMemory(h, env, &page[0], n, nil); err != nil {
			return false, err
		}
		for off := 0; off < int(n); off += 2 {
			ch := binary.LittleEndian.Uint16(page[off:])
			if ch == 0 {
				if pos == len(want)-1 {
					return true, nil
				}
				if pos == 0 {
					return false, nil
				}
				pos = 0
			} else if pos >= 0 && pos < len(want)-1 && want[pos] == ch {
				pos++
			} else {
				pos = -1
			}
		}
		env += n
	}
	return false, nil
}

func scanProcesses(id string, v *inventory) map[int]process {
	m := map[int]process{}
	job, err := openTrackedJob(id)
	if err == nil {
		defer windows.CloseHandle(job)
	} else if !errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
		v.Warning = err.Error()
	}
	snap, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		v.scanErr = err
		v.Warning = err.Error()
		return m
	}
	defer windows.CloseHandle(snap)
	var e windows.ProcessEntry32
	e.Size = uint32(unsafe.Sizeof(e))
	for err = windows.Process32First(snap, &e); err == nil; err = windows.Process32Next(snap, &e) {
		h, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, e.ProcessID)
		if err != nil {
			v.Unreadable++
			continue
		}
		p := process{PID: int(e.ProcessID), PPID: int(e.ParentProcessID), Start: handleStart(h), Command: windows.UTF16ToString(e.ExeFile[:])}
		p.jobObject = job != 0 && inJob(h, job)
		if job == 0 || p.jobObject {
			mem, err := windows.OpenProcess(windows.PROCESS_QUERY_INFORMATION|windows.PROCESS_VM_READ, false, e.ProcessID)
			if err == nil {
				p.Marked, err = hasRun(mem, id)
				windows.CloseHandle(mem)
			}
			if err != nil {
				v.Unreadable++
				p.Unreadable = true
			}
			if p.Marked || p.jobObject {
				buf := make([]byte, 131072)
				if err = windows.NtQueryInformationProcess(h, 60, unsafe.Pointer(&buf[0]), uint32(len(buf)), nil); err == nil {
					p.Command = (*windows.NTUnicodeString)(unsafe.Pointer(&buf[0])).String()
				} else {
					p.Unreadable = true
					v.Unreadable++
				}
			}
		}
		m[p.PID] = p
		windows.CloseHandle(h)
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		v.scanErr = err
		v.Warning = err.Error()
	}
	return m
}
func signalMember(p process, force bool, id string) error {
	h, e := windows.OpenProcess(windows.PROCESS_QUERY_INFORMATION|windows.PROCESS_VM_READ|windows.PROCESS_TERMINATE, false, uint32(p.PID))
	if e != nil {
		return e
	}
	defer windows.CloseHandle(h)
	if !sameProcess(p.Start, handleStart(h)) {
		return os.ErrProcessDone
	}
	match, err := hasRun(h, id)
	if err != nil {
		return err
	}
	if !match {
		return os.ErrPermission
	}
	if force {
		return windows.TerminateProcess(h, 1)
	}
	// Ask each top-level window belonging to this member to close. Console
	// processes without windows receive the full grace interval as well.
	ok, _, err := enumWindows.Call(closeMemberWindow, uintptr(p.PID))
	if ok == 0 {
		return err
	}
	return nil
}

var user32 = windows.NewLazySystemDLL("user32.dll")
var enumWindows = user32.NewProc("EnumWindows")
var windowPID = user32.NewProc("GetWindowThreadProcessId")
var postMessage = user32.NewProc("PostMessageW")
var closeMemberWindow = syscall.NewCallback(func(hwnd, pid uintptr) uintptr {
	var owner uint32
	windowPID.Call(hwnd, uintptr(unsafe.Pointer(&owner)))
	if uintptr(owner) == pid {
		postMessage.Call(hwnd, 0x10, 0, 0)
	} // WM_CLOSE
	return 1
})
