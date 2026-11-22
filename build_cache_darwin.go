package main

import (
	"os"

	"golang.org/x/sys/unix"
)

func cacheMountID(f *os.File) (uint64, error) {
	var stat unix.Statfs_t
	err := unix.Fstatfs(int(f.Fd()), &stat)
	return uint64(uint32(stat.Fsid.Val[0]))<<32 | uint64(uint32(stat.Fsid.Val[1])), err
}
