//go:build darwin || linux

package main

import "os"

func syncSnapshotFile(path string, syncFile func(*os.File) error) error {
	return syncExistingJobFile(path, syncFile)
}
