package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"strings"
)

func snapPortablePath(s string) bool {
	for name := range strings.SplitSeq(s, "/") {
		if name == "" || strings.ContainsAny(name, "<>:\"\\|?*") || strings.TrimRight(name, " .") != name || deviceName.MatchString(name) || strings.ContainsFunc(name, func(r rune) bool { return r < 32 }) {
			return false
		}
	}
	return true
}
func snapWindowsNames(g snapGit, oid string) error {
	raw, e := g.input(nil, "ls-tree", "--name-only", "-rz", oid)
	if e != nil {
		return e
	}
	seen := map[string]string{}
	for _, n := range snapList(string(raw)) {
		if !snapPortablePath(n) {
			return fmt.Errorf("nonportable Windows path: %s", n)
		}
		for prefix := n; prefix != ""; {
			key := strings.ToLower(prefix)
			if v, ok := seen[key]; ok {
				if v != prefix {
					return fmt.Errorf("case collision: %s", prefix)
				}
				break
			}
			seen[key] = prefix
			i := strings.LastIndexByte(prefix, '/')
			if i < 0 {
				break
			}
			prefix = prefix[:i]
		}
	}
	return nil
}
func snapWindowsLinks(g snapGit) error {
	b, e := g.input(nil, "ls-files", "--stage", "-z")
	if e != nil {
		return e
	}
	for _, v := range snapList(string(b)) {
		if strings.HasPrefix(v, "120000 ") {
			_, name, ok := strings.Cut(v, "\t")
			if !ok {
				return errors.New("invalid symlink index entry")
			}
			st, e := os.Lstat(filepath.Join(g.dir, name))
			if e != nil {
				return e
			}
			if st.Mode()&os.ModeSymlink == 0 {
				return fmt.Errorf("native symlink checkout unavailable: %s", name)
			}
		}
	}
	return nil
}

// Resolve all existing ancestors even when the leaf is a dangling link.
func snapLinkInside(root, parent, target string) bool {
	return snapshotLinkInside(root, parent, target, func(p string) (string, bool, error) {
		st, err := os.Lstat(p)
		if os.IsNotExist(err) {
			return "", false, nil
		}
		if err != nil || st.Mode()&os.ModeSymlink == 0 {
			return "", false, err
		}
		next, err := os.Readlink(p)
		return next, true, err
	})
}

func snapshotLinkInside(root, parent, target string, readLink func(string) (string, bool, error)) bool {
	if filepath.IsAbs(target) || filepath.VolumeName(target) != "" {
		return false
	}
	parts := strings.Split(filepath.ToSlash(target), "/")
	links := 0
	for len(parts) > 0 {
		part := parts[0]
		parts = parts[1:]
		switch part {
		case "", ".":
			continue
		case "..":
			parent = filepath.Dir(parent)
			if !snapWithin(root, parent) {
				return false
			}
			continue
		}
		p := filepath.Join(parent, part)
		next, isLink, err := readLink(p)
		if err != nil {
			return false
		}
		if isLink {
			links++
			if links > 64 || filepath.IsAbs(next) || filepath.VolumeName(next) != "" {
				return false
			}
			parts = append(strings.Split(filepath.ToSlash(next), "/"), parts...)
			continue
		}
		parent = p
	}
	return snapWithin(root, parent)
}

func snapWindowsModes(g snapGit, baseline string) error {
	live := snapGit{ctx: g.ctx, dir: g.dir}
	raw, e := live.input(nil, "diff", "--cached", "--raw", "-z", "--no-renames", "--ita-invisible-in-index", baseline, "--")
	if e != nil {
		return e
	}
	entries := snapList(string(raw))
	if len(entries)%2 != 0 {
		return errors.New("invalid raw index diff")
	}
	modes := map[string][]string{}
	for i := 0; i < len(entries); i += 2 {
		meta := strings.Fields(strings.TrimPrefix(entries[i], ":"))
		if len(meta) < 2 {
			return errors.New("invalid index modes")
		}
		if meta[0] == meta[1] || meta[1] != "100755" && meta[1] != "100644" {
			continue
		}
		n := entries[i+1]
		st, statErr := os.Lstat(filepath.Join(g.dir, n))
		if os.IsNotExist(statErr) {
			continue
		}
		if statErr != nil {
			return statErr
		}
		if st.Mode().IsRegular() {
			modes[meta[1]] = append(modes[meta[1]], n)
		}
	}
	for mode, names := range modes {
		flag := "-x"
		if mode == "100755" {
			flag = "+x"
		}
		_, e = g.input([]byte(strings.Join(names, "\x00")+"\x00"), "--literal-pathspecs", "add", "-f", "--chmod="+flag, "--pathspec-from-file=-", "--pathspec-file-nul")
		if e != nil {
			return e
		}
	}
	return nil
}
