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
	for _, k := range []string{"MISE_DATA_DIR", "ASDF_DATA_DIR", "PYENV_ROOT", "RBENV_ROOT"} {
		t.Setenv(k, "")
	}
	shims := filepath.Join(home, ".local", "share", "mise", "shims")
	if err := os.MkdirAll(shims, 0700); err != nil {
		t.Fatal(err)
	}
	sys := "/usr/bin"
	got := shimPath(sys)
	if !strings.HasPrefix(got, shims+string(os.PathListSeparator)) || !strings.HasSuffix(got, sys) {
		t.Fatalf("shimPath = %q", got)
	}
	if again := shimPath(got); again != got {
		t.Fatalf("not idempotent: %q", again)
	}
}
