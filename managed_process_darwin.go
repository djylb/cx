package main

import (
	"errors"

	"golang.org/x/sys/unix"
)

func waitScopedExit(pid int) bool {
	kq, err := unix.Kqueue()
	if err != nil {
		return false
	}
	defer func() { _ = unix.Close(kq) }()
	change := unix.Kevent_t{Ident: uint64(pid), Filter: unix.EVFILT_PROC, Flags: unix.EV_ADD | unix.EV_ONESHOT, Fflags: unix.NOTE_EXIT}
	_, err = unix.Kevent(kq, []unix.Kevent_t{change}, nil, nil)
	if err != nil {
		return errors.Is(err, unix.ESRCH)
	}
	var event [1]unix.Kevent_t
	for {
		n, err := unix.Kevent(kq, nil, event[:], nil)
		if !errors.Is(err, unix.EINTR) {
			return err == nil && n == 1 && event[0].Fflags&unix.NOTE_EXIT != 0
		}
	}
}
