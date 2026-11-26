//go:build windows

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func ownGroup(cmd *exec.Cmd) {}
func killTree(cmd *exec.Cmd) {
	if cmd.Process != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = exec.CommandContext(ctx, "taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid)).Run()
		_ = cmd.Process.Kill()
	}
}

// Keep the psmux console while detaching the worker from its submitter.
func ignoreHangup() {}
func detachWorker(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}
func shellCommand(script string) *exec.Cmd {
	cmd := exec.Command("cmd.exe")
	cmd.SysProcAttr = &syscall.SysProcAttr{CmdLine: `cmd.exe /d /s /c "` + script + `"`}
	return cmd
}

var kernel = windows.NewLazySystemDLL("kernel32.dll")
var resumeProcess = windows.NewLazySystemDLL("ntdll.dll").NewProc("NtResumeProcess")

type commandTree struct {
	job, process windows.Handle
}

func startCommand(cmd *exec.Cmd, id string) (commandTree, error) {
	// Assign before the first instruction so even immediately spawned children
	// remain reachable after the parent exits.
	var name *uint16
	if id != "" {
		var err error
		name, err = windows.UTF16PtrFromString(jobObjectName(id))
		if err != nil {
			return commandTree{}, err
		}
	}
	job, err := windows.CreateJobObject(nil, name)
	if err != nil {
		return commandTree{}, err
	}
	var limits windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err = windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		_ = windows.CloseHandle(job)
		return commandTree{}, fmt.Errorf("set job limits: %w", err)
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_NEW_PROCESS_GROUP | windows.CREATE_SUSPENDED
	if err := cmd.Start(); err != nil {
		_ = windows.CloseHandle(job)
		return commandTree{}, err
	}
	h, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_SUSPEND_RESUME|
		windows.PROCESS_TERMINATE|windows.PROCESS_QUERY_LIMITED_INFORMATION, false, uint32(cmd.Process.Pid))
	if err == nil {
		if err = windows.AssignProcessToJobObject(job, h); err == nil {
			if status, _, _ := resumeProcess.Call(uintptr(h)); status != 0 {
				err = fmt.Errorf("NtResumeProcess: %#x", status)
			}
		}
	}
	if err != nil {
		_ = cmd.Process.Kill()
		_ = waitProcess(cmd, 5*time.Second, nil)
		if h != 0 {
			_ = windows.CloseHandle(h)
		}
		_ = windows.CloseHandle(job)
		return commandTree{}, err
	}
	return commandTree{job, h}, nil
}

type jobAccounting struct {
	_               [4]int64
	_               [2]uint32
	activeProcesses uint32
	_               uint32
}

func stopJob(job windows.Handle) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var info jobAccounting
	for sent := false; ; sent = true {
		if err := windows.QueryInformationJobObject(job, windows.JobObjectBasicAccountingInformation,
			uintptr(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info)), nil); err != nil {
			return fmt.Errorf("query job processes: %w", err)
		}
		if info.activeProcesses == 0 {
			return nil
		}
		if !sent {
			if err := windows.TerminateJobObject(job, 1); err != nil {
				return fmt.Errorf("terminate job: %w", err)
			}
		}
		if err := pause(ctx, 20*time.Millisecond); err != nil {
			return fmt.Errorf("%d processes still in Job Object: %w", info.activeProcesses, err)
		}
	}
}

func (t *commandTree) cleanup() error {
	if t.job == 0 {
		return nil
	}
	// An inventory reader can hold a query handle after the runner closes
	// its handle. Explicit termination also makes normal teardown synchronous.
	err := stopJob(t.job)
	_ = windows.CloseHandle(t.job)
	_ = windows.CloseHandle(t.process)
	*t = commandTree{}
	return err
}
func commandExitCode(state *os.ProcessState) int64 { return int64(uint32(state.ExitCode())) }
