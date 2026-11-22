package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotWindowsReadOnlyPackSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixture.pack")
	if err := os.WriteFile(path, []byte("pack"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	fail := errors.New("injected FlushFileBuffers failure")
	for _, want := range []error{fail, nil} {
		err := syncSnapshotFile(path, func(f *os.File) error {
			if info, err := f.Stat(); err != nil || info.Mode().Perm()&0200 != 0 {
				t.Fatalf("Git pack attribute changed during sync: %v, %v", info, err)
			}
			if err := syncJobFile(f); err != nil {
				return err
			}
			return want
		})
		if !errors.Is(err, want) {
			t.Fatalf("sync result %v, want %v", err, want)
		}
	}
}
