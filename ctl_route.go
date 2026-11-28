package main

import (
	"os"
	"path/filepath"
)

func routePath(id string) (string, error) {
	home, e := controllerCacheRoot()
	if e != nil {
		return "", e
	}
	return jobDir(filepath.Join(home, "routes"), id)
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
	return "local", nil
}

// A single controller cache root determines submission, routing, result,
// progress and observation paths. The node state layout is independent.
func controllerCacheRoot() (string, error) {
	if root := os.Getenv("CX_HOME"); root != "" {
		return filepath.Abs(root)
	}
	cache, err := os.UserCacheDir()
	return filepath.Join(cache, "cx"), err
}
