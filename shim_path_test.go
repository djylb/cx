package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestShimPathPrependsInstalledManagers(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	for _, k := range []string{"MISE_DATA_DIR", "ASDF_DATA_DIR", "ASDF_DIR", "PYENV_ROOT", "RBENV_ROOT"} {
		t.Setenv(k, "")
	}
	shims := filepath.Join(home, ".local", "share", "mise", "shims")
	bin := filepath.Join(home, ".local", "bin")
	for _, d := range []string{shims, bin} {
		if err := os.MkdirAll(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	sys := "/usr/bin"
	got := shimPath(sys)
	if !strings.HasPrefix(got, shims+string(os.PathListSeparator)) || !strings.HasSuffix(got, sys) {
		t.Fatalf("shimPath = %q", got)
	}
	if !strings.Contains(got, bin+string(os.PathListSeparator)) {
		t.Fatalf("manager bin dir missing: %q", got)
	}
	if again := shimPath(got); again != got {
		t.Fatalf("not idempotent: %q", again)
	}
}
