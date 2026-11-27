package main

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Staging names are never handed to observers.
func createJobTemp(dir, prefix string) (*os.File, error) {
	return openJobFile(filepath.Join(dir, prefix+rand.Text()), jobFileNew)
}

func writeJobBytes(path string, data []byte) error {
	f, err := openJobFile(path, jobFileNew)
	if err != nil {
		return err
	}
	return errors.Join(writeFrame(f.Write, data), f.Close())
}

// Sync namespace changes bottom-up. A caller supplies the directory whose
// own creation is already durable as the upper bound.
func syncJobTree(dir, root string) error {
	for {
		if err := syncJobDir(dir); err != nil {
			return err
		}
		parent := filepath.Dir(dir)
		if dir == root || parent == dir {
			return nil
		}
		dir = parent
	}
}

var errJobReplaced = errors.New("job directory or run changed")
var errJobOwnerBusy = errors.New("job has a live owner")

type jobIdentity struct {
	run string
	key string
}

func captureJobIdentity(dir string) (jobIdentity, status, stateCursor, error) {
	return captureJobIdentityWithStatus(dir, loadJobStatus)
}

func captureJobIdentityWithStatus(dir string, read func(string) (status, stateCursor, error)) (jobIdentity, status, stateCursor, error) {
	key, err := jobDirectoryKey(dir)
	if os.IsNotExist(err) {
		err = unregisteredJob(dir)
	}
	if err != nil {
		return jobIdentity{}, status{}, stateCursor{}, err
	}
	s, cursor, err := read(dir)
	if err != nil {
		return jobIdentity{}, s, cursor, err
	}
	id := jobIdentity{s.RunID, key}
	return id, s, cursor, id.checkDir(dir)
}

func (id jobIdentity) checkDir(dir string) error {
	key, err := jobDirectoryKey(dir)
	if os.IsNotExist(err) {
		return errJobReplaced
	}
	if err != nil {
		return err
	}
	if id.key != key {
		return errJobReplaced
	}
	return nil
}

func (id jobIdentity) check(dir string, s status) error {
	if id.run != s.RunID {
		return errJobReplaced
	}
	return id.checkDir(dir)
}

// The path may have moved between open and lock. An already open handle
// always refers to its original inode.
func checkJobHandle(f *os.File, path string) error {
	held, err := f.Stat()
	if err != nil {
		return err
	}
	current, err := os.Stat(path)
	if os.IsNotExist(err) {
		return errJobReplaced
	}
	if err != nil {
		return err
	}
	if !os.SameFile(held, current) {
		return fmt.Errorf("%w: %s", errJobReplaced, path)
	}
	return nil
}

func syncExistingJobFile(path string, sync func(*os.File) error) error {
	f, err := openJobFile(path, jobFileSync)
	if err != nil {
		return err
	}
	return errors.Join(sync(f), f.Close())
}

// A prepared reference is reusable after a crash. Commit the files it
// depends on, including linked Git worktree metadata, before publishing it.
func syncPreparedCheckout(dir string, syncFile func(*os.File) error, syncDir func(string) error) error {
	input := filepath.Join(dir, "input")
	if err := syncExistingJobFile(filepath.Join(input, "manifest.json"), syncFile); err != nil {
		return err
	}
	if err := syncDir(input); err != nil {
		return err
	}
	var worktrees []string
	work := filepath.Join(dir, "work")
	syncPath := func(path string) error { return syncExistingJobFile(path, syncFile) }
	err := syncFileTree(work, syncDir, func(path string) error {
		if filepath.Base(path) != ".git" {
			return syncPath(path)
		}
		data, err := readSharedFile(path)
		if err != nil {
			return err
		}
		target, ok := strings.CutPrefix(strings.TrimRight(string(data), "\r\n"), "gitdir: ")
		if !ok {
			return fmt.Errorf("invalid linked worktree control file %s", path)
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(path), target)
		}
		back, err := readSharedFile(filepath.Join(target, "gitdir"))
		if err != nil {
			return err
		}
		reverse := strings.TrimRight(string(back), "\r\n")
		if !filepath.IsAbs(reverse) {
			reverse = filepath.Join(target, reverse)
		}
		actual, actualErr := os.Stat(path)
		referred, referredErr := os.Stat(reverse)
		if err := errors.Join(actualErr, referredErr); err != nil {
			return err
		}
		if filepath.Base(filepath.Dir(target)) != "worktrees" || !os.SameFile(actual, referred) {
			return fmt.Errorf("invalid linked worktree metadata for %s", path)
		}
		worktrees = append(worktrees, target)
		return syncPath(path)
	})
	if err != nil {
		return err
	}
	for _, admin := range worktrees {
		if err := syncFileTree(admin, syncDir, syncPath); err != nil {
			return err
		}
		if err := syncDir(filepath.Dir(admin)); err != nil {
			return err
		}
		if err := syncDir(filepath.Dir(filepath.Dir(admin))); err != nil {
			return err
		}
	}
	if err := syncExistingJobFile(filepath.Join(dir, "review.diff"), syncFile); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(dir)
}

func syncFileTree(root string, syncDir, syncPath func(string) error) error {
	var directories []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			directories = append(directories, path)
		} else if entry.Type().IsRegular() {
			return syncPath(path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, directorie := range slices.Backward(directories) {
		if err := syncDir(directorie); err != nil {
			return err
		}
	}
	return nil
}
