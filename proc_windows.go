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

var kernel = syscall.NewLazyDLL("kernel32.dll")
var createJob = kernel.NewProc("CreateJobObjectW")
var assignJob = kernel.NewProc("AssignProcessToJobObject")
var terminateJob = kernel.NewProc("TerminateJobObject")
var resumeProcess = syscall.NewLazyDLL("ntdll.dll").NewProc("NtResumeProcess")

type commandTree struct {
	job, process syscall.Handle
}

func startCommand(cmd *exec.Cmd, id string) (commandTree, error) {
	// Assign before the first instruction so even immediately spawned children
	// remain reachable after the parent exits.
	var name *uint16
	if id != "" {
		name, _ = syscall.UTF16PtrFromString(jobObjectName(id))
	}
	job, _, e := createJob.Call(0, uintptr(unsafe.Pointer(name)))
	if job == 0 {
		return commandTree{}, e
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= syscall.CREATE_NEW_PROCESS_GROUP | 4 // CREATE_SUSPENDED
	if err := cmd.Start(); err != nil {
		syscall.CloseHandle(syscall.Handle(job))
		return commandTree{}, err
	}
	h, err := syscall.OpenProcess(0x0100|0x0800|0x0001|0x100000, false, uint32(cmd.Process.Pid))
	if err == nil {
		ok, _, e := assignJob.Call(job, uintptr(h))
		if ok == 0 {
			err = e
		} else if status, _, _ := resumeProcess.Call(uintptr(h)); status != 0 {
			err = fmt.Errorf("NtResumeProcess: %#x", status)
		}
	}
	if err != nil {
		cmd.Process.Kill()
		_ = waitProcess(cmd, 5*time.Second)
		if h != 0 {
			syscall.CloseHandle(h)
		}
		syscall.CloseHandle(syscall.Handle(job))
		return commandTree{}, err
	}
	return commandTree{syscall.Handle(job), h}, nil
}
func (t commandTree) kill() { terminateJob.Call(uintptr(t.job), 1) }
func (t commandTree) cleanup() {
	syscall.CloseHandle(t.job)
	syscall.CloseHandle(t.process)
}
func commandExitCode(state *os.ProcessState) int64 { return int64(uint32(state.ExitCode())) }
