package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPublishBinaryRollback(t *testing.T) {
	for _, scenario := range []string{"restore", "restore-fails", "target-appears"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(dir, "cx.exe")
			staged := filepath.Join(dir, "staged.exe")
			for path, contents := range map[string]string{target: "old", staged: "new"} {
				if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			busy := errors.New("running image is busy")
			publishErr := errors.New("second replacement failed")
			restoreErr := errors.New("restore failed")
			attempts := 0
			replace := func(src, dst string) error {
				attempts++
				if src != staged || dst != target {
					t.Fatalf("replace: %q to %q", src, dst)
				}
				if attempts == 1 {
					return busy
				}
				if _, statErr := os.Lstat(target); !os.IsNotExist(statErr) {
					t.Fatalf("target should be moved aside before the retry: %v", statErr)
				}
				if scenario == "target-appears" {
					if writeErr := os.WriteFile(target, []byte("concurrent"), 0600); writeErr != nil {
						t.Fatal(writeErr)
					}
				}
				return publishErr
			}
			move := func(src, dst string) error {
				if src != target && scenario == "restore-fails" {
					return restoreErr
				}
				// Substitute for Win32 MoveFile, which refuses an existing destination.
				if _, statErr := os.Lstat(dst); statErr == nil {
					return os.ErrExist
				} else if !os.IsNotExist(statErr) {
					return statErr
				}
				return os.Rename(src, dst)
			}
			err := publishWithRollback(staged, target, replace, move, func(err error) bool { return errors.Is(err, busy) })
			if attempts != 2 || !errors.Is(err, publishErr) {
				t.Fatalf("got %d replacement attempts and error %v", attempts, err)
			}
			state, readErr := os.ReadFile(target)
			aside, globErr := filepath.Glob(target + ".old.*")
			if globErr != nil {
				t.Fatal(globErr)
			}
			switch scenario {
			case "restore":
				if readErr != nil || string(state) != "old" || len(aside) != 0 {
					t.Fatalf("old executable was not restored: contents=%q aside=%v err=%v", state, aside, readErr)
				}
			case "restore-fails":
				if !os.IsNotExist(readErr) || !errors.Is(err, restoreErr) || len(aside) != 1 {
					t.Fatalf("rollback failure lost the old image or error: aside=%v err=%v read=%v", aside, err, readErr)
				}
			case "target-appears":
				if readErr != nil || string(state) != "concurrent" || !errors.Is(err, os.ErrExist) || len(aside) != 1 {
					t.Fatalf("rollback clobbered another writer: contents=%q aside=%v err=%v read=%v", state, aside, err, readErr)
				}
			}
			if len(aside) != 0 {
				old, oldErr := os.ReadFile(aside[0])
				if oldErr != nil || string(old) != "old" {
					t.Fatalf("backup contents=%q err=%v", old, oldErr)
				}
			}
		})
	}
}
