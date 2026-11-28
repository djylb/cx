package main

import (
	"path/filepath"
	"slices"
	"testing"
)

func TestLocalModuleTargets(t *testing.T) {
	dir := t.TempDir()
	snapWriteTest(t, filepath.Join(dir, "go.mod"), "module example.com/app\n\nrequire example.com/lib v0.0.0\n\nreplace example.com/lib => ../lib\nreplace example.com/abs => /opt/abs v1.0.0\nreplace example.com/remote => example.com/fork v1.2.3\n")
	snapWriteTest(t, filepath.Join(dir, "go.work"), "go 1.27\n\nuse (\n\t./tools\n)\nuse ../shared\n")
	got := append(localModuleTargets(filepath.Join(dir, "go.mod")), localModuleTargets(filepath.Join(dir, "go.work"))...)
	want := []string{filepath.Join(dir, "..", "lib"), "/opt/abs", filepath.Join(dir, "tools"), filepath.Join(dir, "..", "shared")}
	if !slices.Equal(got, want) {
		t.Fatalf("targets = %v, want %v", got, want)
	}
}
