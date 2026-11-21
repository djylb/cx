package main

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// /proc stat field 22 counts ticks since boot (not subject to wall-clock adjustments).
func procStat(pid int) (p process) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return
	}
	end := bytes.LastIndexByte(b, ')')
	if end < 0 {
		return
	}
	f := strings.Fields(string(b[end+1:]))
	if len(f) < 20 || f[0] == "Z" || f[0] == "X" {
		return
	}
	p.Start, _ = strconv.ParseUint(f[19], 10, 64)
	p.PPID, _ = strconv.Atoi(f[1])
	p.PID = pid
	return
}
func processStart(pid int) uint64 { return procStat(pid).Start }
func scanProcesses(run string, v *inventory) map[int]process {
	m := map[int]process{}
	d, err := os.ReadDir("/proc")
	if err != nil {
		v.scanErr = err
		v.Warning = err.Error()
		return m
	}
	for _, e := range d {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		p := procStat(pid)
		if p.Start == 0 {
			continue
		}
		prefix := "/proc/" + e.Name() + "/"
		env, err := os.ReadFile(prefix + "environ")
		if err != nil && !os.IsNotExist(err) {
			p.Unreadable = true
			v.Unreadable++
		}
		p.Marked = marked(env, run)
		b, _ := os.ReadFile(prefix + "cmdline")
		p.Command = strings.ReplaceAll(strings.TrimRight(string(b), "\x00"), "\x00", " ")
		if sameProcess(p.Start, processStart(pid)) {
			m[pid] = p
		}
	}
	return m
}
func signalMember(p process, force bool, run string) error {
	fd, err := unix.PidfdOpen(p.PID, 0)
	if err == nil {
		defer unix.Close(fd)
	} else if err != unix.ENOSYS && err != unix.EINVAL {
		return err
	}
	if !sameProcess(p.Start, processStart(p.PID)) {
		return os.ErrProcessDone
	}
	env, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", p.PID))
	if err != nil {
		return err
	}
	if !marked(env, run) {
		return os.ErrPermission
	}
	if fd >= 0 {
		return unix.PidfdSendSignal(fd, unixSignal(force), nil, 0)
	}
	if !sameProcess(p.Start, processStart(p.PID)) {
		return os.ErrProcessDone
	}
	return unix.Kill(p.PID, unixSignal(force))
}
