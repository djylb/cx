package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

func birth(k *unix.KinfoProc) uint64 { return uint64(k.Proc.P_starttime.Nano()) }
func processStart(pid int) uint64 {
	k, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil || k.Proc.P_stat == 5 {
		return 0
	}
	return birth(k)
}

// KERN_PROCARGS2 contains argc, padded executable path, argv, then environ.
// Match only whole environment entries, never marker-like command arguments.
func darwinArgs(raw []byte, run string) (string, bool, bool) {
	if len(raw) < 4 {
		return "", false, false
	}
	argc := int(binary.NativeEndian.Uint32(raw))
	i := bytes.IndexByte(raw[4:], 0)
	if i < 0 {
		return "", false, false
	}
	rest := bytes.TrimLeft(raw[5+i:], "\x00")
	var argv []string
	for len(argv) < argc {
		i = bytes.IndexByte(rest, 0)
		if i < 0 {
			return "", false, false
		}
		argv = append(argv, string(rest[:i]))
		rest = rest[i+1:]
	}
	return strings.Join(argv, " "), marked(rest, run), len(bytes.Trim(rest, "\x00")) != 0
}
func scanProcesses(run string, v *inventory) map[int]process {
	m := map[int]process{}
	rows, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		v.Warning = "global process table unavailable; scanning current UID: " + err.Error()
		// Some sandboxes deny the global sysctl but allow proc_listpids and
		// individual kern.proc.pid queries. Limit this fallback to our UID.
		for size := 1024; ; size *= 2 {
			ids := make([]int32, size)
			//lint:ignore SA1019 proc_listpids fallback for CGO-free binaries when kern.proc.all is denied; x/sys exposes no libSystem wrapper.
			//noinspection GoDeprecation
			n, _, errno := unix.Syscall6(unix.SYS_PROC_INFO, 1, 4, uintptr(os.Getuid()), 0, uintptr(unsafe.Pointer(&ids[0])), uintptr(size*4)) //nolint:staticcheck // The CGO-free UID scan fallback has no x/sys libSystem wrapper.
			if errno != 0 {
				v.scanErr = errno
				v.Warning += "; UID scan failed: " + errno.Error()
				return m
			}
			if int(n) >= size*4 {
				continue
			}
			for _, id := range ids[:int(n)/4] {
				k, e := unix.SysctlKinfoProc("kern.proc.pid", int(id))
				if e == nil {
					rows = append(rows, *k)
				} else {
					v.Unreadable++
				}
			}
			break
		}
	}
	for _, k := range rows {
		pid := int(k.Proc.P_pid)
		if pid <= 1 || k.Proc.P_stat == 5 {
			continue
		}
		p := process{PID: pid, PPID: int(k.Eproc.Ppid), Start: birth(&k), Command: unix.ByteSliceToString(k.Proc.P_comm[:])}
		raw, err := unix.SysctlRaw("kern.procargs2", pid)
		cmd, member, ok := darwinArgs(raw, run)
		if err != nil || !ok {
			p.Unreadable = true
			v.Unreadable++
		}
		if cmd != "" {
			p.Command = cmd
		}
		p.Marked = member
		if !member || sameProcess(p.Start, processStart(pid)) {
			m[pid] = p
		}
	}
	return m
}
func signalMember(p process, force bool, run string) error {
	if !sameProcess(p.Start, processStart(p.PID)) {
		return os.ErrProcessDone
	}
	raw, err := unix.SysctlRaw("kern.procargs2", p.PID)
	if err != nil {
		return err
	}
	_, match, _ := darwinArgs(raw, run)
	if !match {
		return os.ErrPermission
	}
	if !sameProcess(p.Start, processStart(p.PID)) {
		return os.ErrProcessDone
	}
	return unix.Kill(p.PID, unixSignal(force))
}
