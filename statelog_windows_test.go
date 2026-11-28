package main

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestStateLogWindowsSharing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), strings.Repeat("a", 130), strings.Repeat("b", 130))
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	w, _ := openTestLogWriter(t, dir)
	w.SetRotateBytes(1)
	if err := w.Append("status", "first"); err != nil {
		t.Fatal(err)
	}
	old := segmentPath(w.dir, 1)
	name, err := windows.UTF16PtrFromString(extendedWindowsPath(old))
	if err != nil {
		t.Fatal(err)
	}
	// Windows rejects the restrictive open itself while this writer is live.
	h, err := windows.CreateFile(name, windows.GENERIC_READ, windows.FILE_SHARE_READ,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
		if err == nil {
			_ = windows.CloseHandle(h)
		}
		t.Fatalf("deny-write reader open = %v", err)
	}
	// A metadata-only open can omit both share flags without requesting a
	// conflicting data handle.
	metadata, err := windows.CreateFile(name, 0, 0, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = windows.CloseHandle(metadata) }()

	// This reader can coexist with the writer but blocks all deletion of
	// segment 1. Cleanup must leave it in place without failing the writer.
	h, err = windows.CreateFile(name, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	reader := os.NewFile(uintptr(h), old)
	defer closeBestEffort(reader)
	if err = w.Append("status", "under a reader"); err != nil {
		t.Fatal(err)
	}
	if err = w.Rotate("config", "first rotation"); err != nil {
		t.Fatal(err)
	}
	if err = w.Append("status", "after checkpoint"); err != nil {
		t.Fatal(err)
	}
	if err = w.Rotate("config", "second rotation"); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(old); err != nil {
		t.Fatalf("protected segment disappeared: %v", err)
	}
	active, err := windows.UTF16PtrFromString(extendedWindowsPath(segmentPath(w.dir, 3)))
	if err != nil {
		t.Fatal(err)
	}
	activeReader, err := windows.CreateFile(active, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = windows.CloseHandle(activeReader) }()
	if err := w.Final("done"); err != nil {
		t.Fatal(err)
	}
	testLogLatest(t, dir, "status", 10, 4)
	var first [1]byte
	if _, err := reader.ReadAt(first[:], 0); err != nil || first[0] != '{' {
		t.Fatalf("protected reader: %q, %v", first, err)
	}
}

func TestWindowsCleanReportsHolderAndIdleTailDoesNotPinState(t *testing.T) {
	h := commands(t)
	h.start("held", nil, "sleep")
	h.cli(0, "close", "--job", "p/held")
	dir := filepath.Join(h.root, "p", "held")
	tail, err := openStateTail(dir, stateCursor{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(tail)
	if _, err = tail.Next(); err != nil {
		t.Fatal(err)
	}
	segment := segmentPath(filepath.Join(dir, "state"), tail.Cursor().Seg)
	holder, err := openJobFile(segment, jobFileRead)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(holder)
	out := h.cli(1, "clean", "--job", "p/held")
	if !strings.Contains(out, segment+": open in PID "+strconv.Itoa(os.Getpid())) {
		t.Fatalf("clean did not identify the open segment and PID: %s", out)
	}
	if err := holder.Close(); err != nil {
		t.Fatal(err)
	}
	h.cli(0, "clean", "--job", "p/held")
}

func TestWindowsRetiredSegmentDoesNotUseLegacyDelete(t *testing.T) {
	dir := t.TempDir()
	f, err := createSegment(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := openJobFile(path, jobFileRead)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(reader)
	var attempted bool
	retireWindowsSegment(path, func(*os.File) error {
		attempted = true
		return windows.ERROR_INVALID_PARAMETER // FileDispositionInfoEx unavailable.
	})
	if !attempted {
		t.Fatal("did not attempt POSIX-style deletion")
	}
	reopened, err := openJobFile(path, jobFileRead)
	if err != nil {
		t.Fatalf("retired segment can no longer be followed: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	// Demonstrate that this sharing mode would permit the old fallback.
	if err := os.Remove(path); err != nil {
		t.Fatalf("legacy fallback would not have been exercised: %v", err)
	}
}

func TestWindowsHistoryBarrierRetriesAfterDenyWriteReader(t *testing.T) {
	dir := fixture(t, "running")
	path := segmentPath(filepath.Join(dir, "state"), 1)
	name, err := windows.UTF16PtrFromString(extendedWindowsPath(path))
	if err != nil {
		t.Fatal(err)
	}
	h, err := windows.CreateFile(name, windows.GENERIC_READ, windows.FILE_SHARE_READ|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	reader := os.NewFile(uintptr(h), path)
	defer closeBestEffort(reader)
	var s status
	if err = projected(t.Context(), dir, &s); !errors.Is(err, errStateHistoryBusy) || !strings.Contains(err.Error(), "000001.jsonl") {
		t.Fatalf("history sharing conflict was not retryable or identifiable: %v", err)
	}
	s, _, err = loadJobStatus(dir)
	if err != nil || s.final {
		t.Fatalf("published an orphan without the history barrier: %+v %v", s, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := projected(t.Context(), dir, &s); err != nil || !s.final || s.State != "orphaned" {
		t.Fatalf("orphan publication could not retry: %+v %v", s, err)
	}
}

func TestWindowsStateSegmentRepeatedSyncWithReader(t *testing.T) {
	dir := t.TempDir()
	w, _ := openTestLogWriter(t, dir)
	reader, err := openJobFile(segmentPath(w.dir, 1), jobFileRead)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(reader)
	for _, value := range []string{"first", "second"} {
		if err := w.Append("status", value); err != nil {
			t.Fatal(err)
		}
		if err := w.sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Final("terminal"); err != nil {
		t.Fatal(err)
	}
	testLogLatest(t, dir, "status", 3, 2)
}
