package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSnapshotOwnership(t *testing.T) {
	ctx := snapTestCtx(t)
	src := filepath.Join(t.TempDir(), "repo")
	snapWriteTest(t, filepath.Join(src, "owned-dir", "keep"), "data")
	snapWriteTest(t, filepath.Join(src, "owned file"), "data")
	snapWriteTest(t, filepath.Join(src, "other"), "data")
	linkErr := os.Symlink("owned-dir", filepath.Join(src, "link"))
	sid, manifest, freezeErr := Freeze(ctx, src, FreezeOptions{})
	if freezeErr != nil {
		t.Fatal(freezeErr)
	}
	paths := []string{"owned-dir", "owned file"}
	if validateErr := validateSnapshotOwnership(ctx, sid, paths); validateErr != nil {
		t.Fatal(validateErr)
	}
	invalid := []string{"../outside", src, "", "missing"}
	if linkErr == nil {
		invalid = append(invalid, "link", "link/keep")
	}
	for _, path := range invalid {
		if validateErr := validateSnapshotOwnership(ctx, sid, []string{path}); validateErr == nil {
			t.Errorf("accepted --own %q", path)
		}
	}
	index, path, memberErr := ownedMember("top/app/subdir", []string{"top/app", "top/lib"}, "../lib/file")
	if memberErr != nil || index != 1 || path != "file" {
		t.Fatalf("sibling lookup = %d, %q, %v", index, path, memberErr)
	}

	root := t.TempDir()
	store, storeErr := snapStore()
	if storeErr != nil {
		t.Fatal(storeErr)
	}
	prep, prepErr := Materialize(ctx, filepath.Join(root, ".work", "p", "job"), manifest, store)
	if prepErr != nil {
		t.Fatal(prepErr)
	}
	tmp := t.TempDir()
	r := runner{root: root, c: config{Dir: prep.Source, Sandbox: "workspace-write", Own: paths, Roots: []string{tmp}}}
	params, paramsErr := r.codexThreadParams()
	if paramsErr != nil {
		t.Fatal(paramsErr)
	}
	dir, file := filepath.Join(prep.Source, paths[0]), filepath.Join(prep.Source, paths[1])
	filesystem := fields{":root": "read", dir: "write", file: "write", tmp: "write"}
	profile := params["config"].(fields)["permissions."+ownedPermissions].(fields)
	if params["permissions"] != ownedPermissions || params["sandbox"] != nil || !reflect.DeepEqual(profile["filesystem"], filesystem) {
		t.Fatalf("unexpected Codex policy: %v", params)
	}
	args := r.c.claudeArgs("", "", dir, file)
	var settings struct {
		Sandbox struct {
			Filesystem                                           struct{ AllowWrite []string }
			Enabled, FailIfUnavailable, AllowUnsandboxedCommands bool
		}
	}
	if jsonErr := json.Unmarshal([]byte(flagValue(args, "--settings")), &settings); jsonErr != nil {
		t.Fatal(jsonErr)
	}
	if flagValue(args, "--add-dir") != dir || !settings.Sandbox.Enabled || !settings.Sandbox.FailIfUnavailable ||
		settings.Sandbox.AllowUnsandboxedCommands || !reflect.DeepEqual(settings.Sandbox.Filesystem.AllowWrite, []string{dir, file, tmp}) {
		t.Fatalf("unexpected Claude policy: %v", args)
	}
}
