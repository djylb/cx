//go:build !windows

package main

import (
	"os"
	"syscall"
)

func privateSSHDir(info os.FileInfo) bool {
	s, ok := info.Sys().(*syscall.Stat_t)
	return ok && s.Uid == uint32(os.Getuid()) && info.Mode().Perm()&0077 == 0
}
