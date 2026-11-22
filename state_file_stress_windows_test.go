package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Replacing a state file while other handles read it must never leave the old
// contents visible to a later open.
func TestWindowsReplaceUnderReaders(t *testing.T) {
	variants := map[string]func(from, to string) error{
		"replaceFile": replaceFile,
		"posix":       renameFilePOSIX,
		"movefile": func(from, to string) error {
			return retryWindowsFileIO(stateReplaceBudget, isWindowsFileConflict, func() error { return os.Rename(from, to) })
		},
	}
	report := ""
	failed := false
	for name, replace := range variants {
		dir := t.TempDir()
		path := filepath.Join(dir, "status.json")
		if err := os.WriteFile(path, []byte("0"), 0600); err != nil {
			t.Fatal(err)
		}
		var stop atomic.Bool
		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() {
				for !stop.Load() {
					_, _ = readStateFile(path)
				}
			})
		}
		stale, errs := 0, 0
		var first string
		start := time.Now()
		for i := 1; i <= 500; i++ {
			tmp := filepath.Join(dir, fmt.Sprintf(".w%d", i))
			if err := os.WriteFile(tmp, []byte(fmt.Sprint(i)), 0600); err != nil {
				t.Fatal(err)
			}
			if err := replace(tmp, path); err != nil {
				errs++
				if first == "" {
					first = err.Error()
				}
				_ = os.Remove(tmp)
				continue
			}
			b, err := readStateFile(path)
			if err != nil || string(b) != fmt.Sprint(i) {
				stale++
				if first == "" {
					first = fmt.Sprintf("write %d read %q %v", i, b, err)
				}
			}
		}
		stop.Store(true)
		wg.Wait()
		report += fmt.Sprintf("%s: stale %d errors %d in %v first %q\n", name, stale, errs, time.Since(start), first)
		if stale > 0 {
			failed = true
		}
	}
	if failed {
		t.Fatal(report)
	}
	t.Log(report)
}
