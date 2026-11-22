package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func cacheMountID(f *os.File) (uint64, error) {
	var stat unix.Statx_t
	if err := unix.Statx(int(f.Fd()), "", unix.AT_EMPTY_PATH, unix.STATX_MNT_ID, &stat); err == nil && stat.Mask&unix.STATX_MNT_ID != 0 {
		return stat.Mnt_id, nil
	}
	// Older kernels can report the same mount identifier in fdinfo. Device
	// numbers alone cannot detect a bind mount of the same filesystem.
	b, err := readSharedFile(fmt.Sprintf("/proc/self/fdinfo/%d", f.Fd()))
	if err != nil {
		return 0, err
	}
	for line := range strings.SplitSeq(string(b), "\n") {
		if id, ok := strings.CutPrefix(line, "mnt_id:"); ok {
			return strconv.ParseUint(strings.TrimSpace(id), 10, 64)
		}
	}
	return 0, fmt.Errorf("mount ID unavailable for %s", f.Name())
}
