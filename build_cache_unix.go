//go:build darwin || linux

package main

import (
	"os"
	"os/exec"
	"slices"
	"syscall"
)

func cacheDirectory(info os.FileInfo) bool { return info.IsDir() }

func cacheProcessGroup(cmd *exec.Cmd) int { return cmd.Process.Pid }

func cacheRunAlive(run string, groups []int) bool {
	view := inventory{}
	procs := scanProcesses(run, &view)
	if view.scanErr != nil {
		return true // Process visibility is required before a pin can expire.
	}
	for _, p := range procs {
		if p.Marked {
			return true
		}
		if len(groups) > 0 {
			if group, err := syscall.Getpgid(p.PID); err == nil && slices.Contains(groups, group) {
				return true
			}
		}
	}
	return false
}

// flock follows the open file description shared by a runner and its child.
// This keeps a direct lease even if a subprocess clears its environment.
// Process pins are also needed for launchers that close inherited descriptors.
func inheritCacheLeases(cmd *exec.Cmd, leases []*os.File) {
	cmd.ExtraFiles = append(cmd.ExtraFiles, leases...)
}
