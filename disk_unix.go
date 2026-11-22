//go:build darwin || linux

package main

import "golang.org/x/sys/unix"

func diskSpace(path string) (free, total uint64, err error) {
	var stat unix.Statfs_t
	err = unix.Statfs(path, &stat)
	return stat.Bavail * uint64(stat.Bsize), stat.Blocks * uint64(stat.Bsize), err
}
