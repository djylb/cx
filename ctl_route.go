package main

import (
	"os"
	"path/filepath"
)

func routePath(id string) (string, error) {
	home, e := os.UserHomeDir()
	if e != nil {
		return "", e
	}
	return jobDir(filepath.Join(home, ".cache", "cx", "routes"), id)
}
func rememberRoute(id, host string) error {
	path, e := routePath(id)
	if e != nil {
		return e
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	return writeJSON(path, host)
}
func lookupRoute(id string) (string, error) {
	path, e := routePath(id)
	if e != nil {
		return "", e
	}
	var host string
	e = readJSON(path, &host)
	if e == nil {
		return host, nil
	}
	if !os.IsNotExist(e) {
		return "", e
	}
	path, e = legacyRouteFile(id)
	if os.IsNotExist(e) {
		return "local", nil
	}
	if e != nil {
		return "", e
	}
	var old struct{ Host string }
	if e = readJSON(path, &old); e == nil {
		return old.Host, nil
	} else if !os.IsNotExist(e) {
		return "", e
	}
	return "local", nil
}

// Python routes are JSON despite the historical .env suffix.
func legacyRouteFile(id string) (string, error) {
	root := os.Getenv("CX_HOME")
	if root == "" {
		return "", os.ErrNotExist
	}
	path, e := jobDir(filepath.Join(root, ".locks", "cx-jobs"), id)
	return path + ".env", e
}
