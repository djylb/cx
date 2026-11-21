//go:build !windows

package main

import (
	"os"
	"os/exec"
	"os/signal"
	"syscall"
)

// The app-server runs in its own process group so that a stall restart can kill its commands too.
func ownGroup(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }
func killTree(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Process.Kill()
	}
}

func ignoreHangup()                        { signal.Ignore(syscall.SIGHUP) }
func detachWorker(cmd *exec.Cmd)           { cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} }
func shellCommand(script string) *exec.Cmd { return exec.Command("/bin/sh", "-c", script) }

type commandTree struct{}

func startCommand(cmd *exec.Cmd, _ string) (commandTree, error) {
	ownGroup(cmd)
	if err := cmd.Start(); err != nil {
		return commandTree{}, err
	}
	return commandTree{}, nil
}
func (commandTree) cleanup()                       {}
func commandExitCode(state *os.ProcessState) int64 { return int64(state.ExitCode()) }
