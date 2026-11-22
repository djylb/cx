package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func cacheAccessTime(info os.FileInfo) time.Time {
	stamp := info.Sys().(*syscall.Stat_t).Atim
	return time.Unix(stamp.Unix())
}

func stateDiskLocal(path string) (bool, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return false, err
	}
	switch uint32(stat.Type) {
	case unix.NFS_SUPER_MAGIC, unix.CIFS_SUPER_MAGIC, unix.SMB2_SUPER_MAGIC,
		unix.V9FS_MAGIC, unix.AFS_SUPER_MAGIC, unix.CEPH_SUPER_MAGIC,
		unix.CODA_SUPER_MAGIC, unix.NCP_SUPER_MAGIC:
		return false, nil
	case unix.FUSE_SUPER_MAGIC:
		// FUSE's magic is shared by local and network implementations.
		return localFUSEMount(path)
	default:
		return true, nil
	}
}

func localFUSEMount(path string) (bool, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return false, err
	}
	mounts, err := readSharedFile("/proc/self/mountinfo")
	if err != nil {
		return false, err
	}
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	var mount, fs string
	lines := bufio.NewScanner(strings.NewReader(string(mounts)))
	for lines.Scan() {
		parts := strings.Fields(lines.Text())
		if len(parts) < 10 {
			continue
		}
		at := unescape.Replace(parts[4])
		if len(at) < len(mount) || !snapWithin(at, resolved) {
			continue
		}
		_, tail, ok := strings.Cut(lines.Text(), " - ")
		if ok {
			mount = at
			fs, _, _ = strings.Cut(tail, " ")
		}
	}
	if err := lines.Err(); err != nil {
		return false, err
	}
	// Only a FUSE implementation known to wrap local storage permits a
	// positive answer. A generic subtype can hide a user-space network mount.
	switch fs {
	case "fuseblk":
		return true, nil
	case "fuse.sshfs", "fuse.rclone", "fuse.s3fs", "fuse.glusterfs":
		return false, nil
	default:
		return false, fmt.Errorf("cannot confirm FUSE state volume is local (%s)", fs)
	}
}
