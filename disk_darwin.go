package main

import (
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func cacheAccessTime(info os.FileInfo) time.Time {
	stamp := info.Sys().(*syscall.Stat_t).Atimespec
	return time.Unix(stamp.Unix())
}

func stateDiskLocal(path string) (bool, error) {
	var stat unix.Statfs_t
	err := unix.Statfs(path, &stat)
	return stat.Flags&unix.MNT_LOCAL != 0, err
}
