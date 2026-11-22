package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsStateFileSharedReads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "status.json")
	if err := writeJSON(path, "old"); err != nil {
		t.Fatal(err)
	}
	reader, err := openShared(path)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(reader)

	// Opening with all three access rights requires the reader to share each one.
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := windows.CreateFile(name, windows.GENERIC_READ|windows.GENERIC_WRITE|windows.DELETE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = windows.CloseHandle(h); err != nil {
		t.Fatal(err)
	}

	if err = writeJSON(path, "new"); err != nil {
		t.Fatal(err)
	}
	old, err := io.ReadAll(reader)
	if err != nil || string(old) != "\"old\"\n" {
		t.Fatalf("open handle read %q, %v", old, err)
	}
	var got string
	if err = readJSON(path, &got); err != nil || got != "new" {
		t.Fatalf("new handle read %q, %v", got, err)
	}
}

func TestWindowsStateFileRetry(t *testing.T) {
	t.Run("replace", func(t *testing.T) {
		outcomes := []error{
			&os.LinkError{Op: "rename", Err: windows.ERROR_SHARING_VIOLATION},
			windows.ERROR_ACCESS_DENIED,
			nil,
		}
		attempts := 0
		err := retryWindowsFileIO(stateReplaceBudget, isWindowsFileConflict, func() error {
			err := outcomes[attempts]
			attempts++
			return err
		})
		if err != nil || attempts != len(outcomes) {
			t.Fatalf("replace attempted %d times, error %v", attempts, err)
		}
	})

	t.Run("open", func(t *testing.T) {
		attempts := 0
		err := retryWindowsFileIO(stateReadBudget, isWindowsOpenTransient, func() error {
			attempts++
			if attempts == 1 {
				return windows.ERROR_FILE_NOT_FOUND
			}
			return nil
		})
		if err != nil || attempts != 2 {
			t.Fatalf("open attempted %d times, error %v", attempts, err)
		}
	})

	t.Run("expired", func(t *testing.T) {
		attempts := 0
		err := retryWindowsFileIO(0, isWindowsFileConflict, func() error {
			attempts++
			return windows.ERROR_ACCESS_DENIED
		})
		if !errors.Is(err, windows.ERROR_ACCESS_DENIED) || attempts != 1 {
			t.Fatalf("expired replace attempted %d times, error %v", attempts, err)
		}
	})
}

func TestWindowsStateFileErrorsNamePath(t *testing.T) {
	dir := t.TempDir()
	from, to := filepath.Join(dir, "missing"), filepath.Join(dir, "status.json")
	err := replaceFile(from, to)
	var linkErr *os.LinkError
	if !errors.As(err, &linkErr) || linkErr.Old != from || linkErr.New != to || !os.IsNotExist(err) {
		t.Fatalf("replaceFile returned %v", err)
	}
	_, err = readStateFile(to)
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) || pathErr.Path != to || !os.IsNotExist(err) {
		t.Fatalf("readStateFile returned %v", err)
	}
}

// The POSIX rename request is the primary replacement path; report exactly how a
// volume treats it so a fallback is never silently relied upon.
func TestWindowsPOSIXRenameReplaces(t *testing.T) {
	t.Run("idle target", func(t *testing.T) { checkPOSIXRename(t, false) })
	t.Run("open target", func(t *testing.T) { checkPOSIXRename(t, true) })
}

func checkPOSIXRename(t *testing.T, open bool) {
	dir := t.TempDir()
	from, to := filepath.Join(dir, "from"), filepath.Join(dir, "to")
	for _, f := range []struct{ path, body string }{{from, "new"}, {to, "old"}} {
		if err := os.WriteFile(f.path, []byte(f.body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if open {
		reader, err := openShared(to)
		if err != nil {
			t.Fatal(err)
		}
		defer closeBestEffort(reader)
	}
	err := renameFilePOSIX(from, to)
	got, readErr := os.ReadFile(to)
	entries, _ := os.ReadDir(dir)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if err != nil || readErr != nil || string(got) != "new" || len(names) != 1 {
		root, _ := windows.UTF16PtrFromString(filepath.VolumeName(dir) + `\`)
		fs := make([]uint16, 64)
		volErr := windows.GetVolumeInformation(root, nil, 0, nil, nil, nil, &fs[0], uint32(len(fs)))
		t.Fatalf("dir %s (%s %v): rename error %v, target %q (%v), entries %q", dir, windows.UTF16ToString(fs), volErr, err, got, readErr, names)
	}
}
