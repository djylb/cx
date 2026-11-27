package main

import (
	"errors"
	"os"
)

func syncSnapshotFile(path string, syncFile func(*os.File) error) error {
	f, err := openJobFile(path, jobFileSync)
	if errors.Is(err, os.ErrPermission) {
		// Git marks packs and loose objects read-only. Windows requires
		// a write handle to flush them. The caller holds the import lock;
		// restore the attribute before writing the durability barrier.
		info, statErr := os.Stat(path)
		if statErr != nil || info.Mode().Perm()&0200 != 0 {
			return errors.Join(err, statErr)
		}
		mode := info.Mode().Perm()
		if err = os.Chmod(path, mode|0200); err != nil {
			return err
		}
		f, err = openJobFile(path, jobFileSync)
		restoreErr := os.Chmod(path, mode)
		if err != nil {
			return errors.Join(err, restoreErr)
		}
		if restoreErr != nil {
			return errors.Join(restoreErr, f.Close())
		}
	}
	if err != nil {
		return err
	}
	return errors.Join(syncFile(f), f.Close())
}
