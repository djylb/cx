package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

type packageCacheRecord struct {
	Env           map[string]string
	WritableRoots []string
}

func packageCacheFixtureEnv() map[string]string {
	env := map[string]string{}
	for _, key := range []string{"npm_config_cache", "PNPM_STORE_DIR", "npm_config_store_dir", "YARN_CACHE_FOLDER", "PIP_CACHE_DIR", "CARGO_HOME", "GOCACHE", "GOMODCACHE", "GOPATH", "STATICCHECK_CACHE", "GOLANGCI_LINT_CACHE", "TMPDIR"} {
		env[key] = os.Getenv(key)
	}
	return env
}

func TestPackageCachesAcrossJobs(t *testing.T) {
	_, work, prompt := runnerFixture(t)
	h := commands(t)
	home := t.TempDir()
	for _, name := range []string{"HOME", "USERPROFILE", "LOCALAPPDATA", "MISE_DATA_DIR", "ASDF_DATA_DIR", "ASDF_DIR", "PYENV_ROOT", "RBENV_ROOT"} {
		t.Setenv(name, home)
	}
	t.Setenv("FIXMODE", "wait-followup")
	t.Setenv("npm_config_cache", filepath.Join(work, "checkout-cache"))
	root := filepath.Join(h.root, "cache", "packages")
	want := map[string]string{
		"npm_config_cache":     filepath.Join(root, "npm"),
		"PNPM_STORE_DIR":       filepath.Join(root, "pnpm"),
		"npm_config_store_dir": filepath.Join(root, "pnpm"),
		"YARN_CACHE_FOLDER":    filepath.Join(root, "yarn"),
		"PIP_CACHE_DIR":        filepath.Join(root, "pip"),
		"CARGO_HOME":           filepath.Join(root, "cargo"),
		"GOCACHE":              filepath.Join(root, "go-build"),
		"GOMODCACHE":           filepath.Join(root, "go-mod"),
		"GOPATH":               filepath.Join(root, "go-path"),
		"STATICCHECK_CACHE":    filepath.Join(root, "staticcheck"),
		"GOLANGCI_LINT_CACHE":  filepath.Join(root, "golangci-lint"),
	}
	for i, id := range []string{"first/cache-test", "second/cache-test"} {
		recordPath := filepath.Join(t.TempDir(), "cache-record.json")
		t.Setenv("FIXTURE_PACKAGE_CACHE_RECORD", recordPath)
		h.cli(0, "start", "--job", id, "--dir", work, "--prompt-file", prompt, "--sandbox", "workspace-write", "--network", "--idle-exit", "200ms")
		var record packageCacheRecord
		if err := readJSON(recordPath, &record); err != nil {
			t.Fatal(err)
		}
		tmp := filepath.Join(h.root, filepath.FromSlash(id), "tmp")
		if record.Env["TMPDIR"] != tmp || !slices.Contains(record.WritableRoots, tmp) {
			t.Errorf("%s: TMPDIR = %q, writable roots = %v, want %q", id, record.Env["TMPDIR"], record.WritableRoots, tmp)
		}
		for key, path := range want {
			if record.Env[key] != path || !slices.Contains(record.WritableRoots, path) {
				t.Fatalf("%s: %s = %q, writable roots = %v, want %q", id, key, record.Env[key], record.WritableRoots, path)
			}
			marker := filepath.Join(path, "cache-marker")
			if i == 0 {
				if err := os.WriteFile(marker, nil, 0600); err != nil {
					t.Fatal(err)
				}
			} else if _, err := os.Stat(marker); err != nil {
				t.Fatal(err)
			}
		}
	}
	if info, err := os.Stat(filepath.Join(want["CARGO_HOME"], "registry")); err != nil || !info.IsDir() {
		t.Fatalf("Cargo registry directory: %v, %v", info, err)
	}

	override := filepath.Join(t.TempDir(), "go-mod-override")
	h.start("package-cache", []string{"--env", "GOMODCACHE=" + override}, "package-cache-env")
	h.cli(0, "wait", "--job", "p/package-cache", "--timeout", "2m")
	var env map[string]string
	if err := readJSON(filepath.Join(h.root, "p", "package-cache", "output.log"), &env); err != nil {
		t.Fatal(err)
	}
	want["GOMODCACHE"] = override
	for key, path := range want {
		if env[key] != path {
			t.Fatalf("command %s = %q, want %q", key, env[key], path)
		}
	}
	for _, id := range []string{"first/cache-test", "second/cache-test"} {
		tmp := filepath.Join(h.root, filepath.FromSlash(id), "tmp")
		makeReadOnlyCacheFixture(t, tmp)
		op := "close"
		if id == "second/cache-test" {
			op = "clean"
		}
		h.cli(0, op, "--job", id)
		if _, err := os.Stat(tmp); !os.IsNotExist(err) {
			t.Errorf("%s should remove %s: %v", op, tmp, err)
		}
	}
}
