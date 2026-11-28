package main

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
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
func lookupRoute(ctx context.Context, id string) (string, error) {
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
	return discoverRoute(ctx, id)
}

// discoverRoute finds the host of a job this controller did not route itself
// (another session, a resumed or adopted job): cached listings first, then a
// short live query. One match is remembered; several are an error naming them.
func discoverRoute(ctx context.Context, id string) (string, error) {
	hs, err := loadEndpoints()
	if err != nil {
		return "local", nil
	}
	match := func(rows []fields) bool {
		return slices.ContainsFunc(rows, func(row fields) bool { return listingJobID(row) == id })
	}
	var found []string
	for _, name := range slices.Sorted(maps.Keys(hs)) {
		if path, cacheErr := lsCachePath(name); cacheErr == nil {
			if snapshot, loadErr := loadLSSnapshot(path); loadErr == nil && match(snapshot.Jobs) {
				found = append(found, name)
			}
		}
	}
	if len(found) == 0 {
		project, _, _ := strings.Cut(id, "/")
		names := slices.Sorted(maps.Keys(hs))
		hits := make([]bool, len(names))
		var wg sync.WaitGroup
		for i, name := range names {
			wg.Go(func() {
				observing, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				r, callErr := hs[name].call(observing, 5*time.Second, "rows", project, "")
				var rows []fields
				hits[i] = callErr == nil && r.code == 0 && decodeRows(r.data, &rows) == nil && match(rows)
			})
		}
		wg.Wait()
		for i, hit := range hits {
			if hit {
				found = append(found, names[i])
			}
		}
	}
	switch len(found) {
	case 0:
		return "local", nil
	case 1:
		if rememberErr := rememberRoute(id, found[0]); rememberErr != nil {
			diagnosticf("cx: remember route for %s: %v\n", id, rememberErr)
		}
		return found[0], nil
	}
	choices := make([]string, len(found))
	for i, name := range found {
		choices[i] = name + ":" + id
	}
	return "", fmt.Errorf("job %s exists on several hosts; use one of: %s", id, strings.Join(choices, ", "))
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
