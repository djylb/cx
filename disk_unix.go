//go:build darwin || linux

package main

import "golang.org/x/sys/unix"

func diskSpace(path string) (free, total uint64, err error) {
	var stat unix.Statfs_t
	err = unix.Statfs(path, &stat)
	return uint64(stat.Bavail) * uint64(stat.Bsize), uint64(stat.Blocks) * uint64(stat.Bsize), err
}
