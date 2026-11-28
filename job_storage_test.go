package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestJobStorageDoesNotReplaceFiles(t *testing.T) {
	// The counters make new calls in an already allowed function explicit.
	// Exceptions are for private controller files, transport, deployment,
	// snapshot storage, and publication/cleanup of whole directories.
	allowed := map[string]map[string]int{
		// These records are private cache maintenance metadata outside all job directories.
		"build_cache.go": {"prepareCaches": 1, "prepareCacheEntry": 1, "scanCaches": 1, "startScopedCommand": 1},
		"main.go":        {"atomicWrite": 1, "atomicWriteSync": 1, "writeJSON": 1, "detachCleanJob": 1},
		"ctl_apply.go":   {"installPlans": 2, "applyTurn": 2},
		"ctl_center.go":  {"freezeCompilerSource": 2, "compileSnapshot": 1},
		"ctl_chat.go":    {"ctlChat": 1},
		// Controller configuration mirrors, outside every job directory.
		"ctl_context.go":         {"writeConfigFile": 1, "pushContext": 1},
		"ctl_deploy.go":          {"configRelease": 1, "rememberPlatformVersion": 1, "cacheArtifact": 1, "cacheNode": 1},
		"ctl_follow.go":          {"follow": 1},
		"ctl_incremental.go":     {"incrementalResult": 1},
		"ctl_install_unix.go":    {"publishBinary": 1},
		"ctl_install_windows.go": {"replaceBinary": 1},
		"ctl_journal.go":         {"journalDecision": 1},
		"ctl_node.go":            {"hostSettings": 1},
		"ctl_observe.go":         {"ctlWaitAny": 1, "saveLSSnapshot": 1}, // Controller-local ls cache, replaced whole.
		"ctl_project.go":         {"ctlProject": 1},
		"ctl_queue.go":           {"deployJob": 1, "submissionWorker": 3, "queueJob": 2},                            // Controller request, queued progress, and confirmed local launch failure under the schedule lock.
		"ctl_rebase.go":          {"ctlRebase": 3, "recordRebaseFailure": 1, "dispatchRebase": 1, "claimRebase": 1}, // Controller-local pending rebase requests, replaced whole.
		"ctl_result.go":          {"sealTurn": 1, "unpackTurn": 1, "publishResultMarker": 1, "forgetResult": 1, "Write": 1},
		// Context output is published as one immutable directory per baseline.
		"runner_modules.go": {"prepareModuleContexts": 1},
		// Controller transport history rotates complete private log files.
		"transport_log.go": {"recordTransportSample": 1},
		"ctl_route.go":     {"rememberRoute": 1},
		"ctl_transfer.go":  {"receiveTask": 1},
		"snap_publish.go":  {"snapPrerequisites": 1, "snapArchive": 1, "Publish": 1},
		"wait_any.go":      {"updateWaitCursor": 1, "registerWaitObserver": 1}, // Controller cursor and observer record.
		"ctl_refresh.go":   {"ctlRefresh": 2},                                  // Controller-local pending refresh request.
		"node_refresh.go":  {"refreshCheckout": 1, "commitRefresh": 1},         // Private checkout baseline marker and input.
	}
	// Module contexts and turn metadata use exclusive staging files and
	// immutable links. Read histories derive from turn references. Frozen
	// deliveries publish whole private directories before and after checks.
	// Every standard-library file open, including references through a
	// function variable, must belong to one of these non-job scopes.
	rawAllowed := map[string]map[string]int{
		"ctl_apply.go":       {"planApply": 4, "installPlans": 2, "applyTurn": 1},
		"ctl_baseline.go":    {"submitRebase": 1, "rebaseAttachments": 3}, // Controller-local sealed results and prompt file.
		"ctl_center.go":      {"freezeCompilerSource": 2},
		"ctl_cli.go":         {"preview": 1},
		"ctl_deploy.go":      {"cacheArtifact": 1, "writeStream": 1, "copyFile": 1, "moduleDir": 1},
		"ctl_incremental.go": {"incrementalResult": 1, "incrementalPatch": 1},
		"node_refresh.go":    {"planRefresh": 2}, // Git index of the private checkout, as in planApply.
		"ctl_journal.go":     {"journalDecision": 1},
		"ctl_queue.go":       {"queueJob": 1},
		"ctl_setup.go":       {"receive": 1, "fileHash": 1, "installNode": 2, "upload": 1},
		"ctl_ssh.go":         {"current": 1, "next": 1},
		"ctl_submit.go":      {"controllerPolicy": 1, "ctlSubmitWithRebase": 1, "submitBatch": 1},
		"facts.go":           {"osVersion": 1, "platformFacts": 3, "fingerprint": 1},
		"main.go":            {"atomicWriteSync": 2},
		"snap_freeze.go":     {"snapStage": 1, "snapCapture": 1},
		"snap_layout.go":     {"snapLayout": 2},
		"snap_publish.go": {"snapPrerequisites": 2, "snapArchive": 2, "writeSnapshotArchive": 1,
			"receivePacket": 1, "sendPacket": 1},
		"statelog_unix.go":  {"openJobFile": 1},
		"tracking_linux.go": {"procStat": 1, "scanProcesses": 2, "signalMember": 1},
	}
	rawNames := map[string]bool{"Open": true, "OpenFile": true, "ReadFile": true, "Create": true,
		"CreateTemp": true, "WriteFile": true}
	// Check the transitive call graph from the lifecycle entry points. The
	// explicit stops are OS/transport/cache functions or shared-open itself.
	boundary := map[string]bool{"openJobFile": true, "planRefresh": true, "installPlans": true, // refresh edits only the private checkout
		"atomicWrite": true, "writeJSON": true,
		"platformFacts": true, "osVersion": true, "fingerprint": true, "fileHash": true,
		"procStat": true, "scanProcesses": true, "signalMember": true,
		"preinstallDoctor": true, "installNode": true, "receiveTask": true, "cacheNode": true,
		"submitScript": true, "rememberRoute": true, "loadEndpoints": true,
		"controller": true, "nodeCLI": true, "transportCommand": true, "retire": true,
		"loadSnapshot": true, "ReceiveSnapshot": true, "sendPacket": true,
		"writeStream": true, "copyFile": true, "prepareCaches": true}
	type body struct {
		calls []string
		raw   []token.Position
	}
	funcs := make(map[string][]body)
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		osNames := make(map[string]bool)
		for _, spec := range file.Imports {
			if spec.Path.Value == `"os"` {
				if spec.Name == nil {
					osNames["os"] = true
				} else {
					osNames[spec.Name.Name] = true
					if spec.Name.Name == "." {
						t.Errorf("%s: dot imports bypass the storage guard", name)
					}
				}
			}
		}
		for _, decl := range file.Decls {
			fn, _ := decl.(*ast.FuncDecl)
			if fn == nil || fn.Body == nil {
				continue
			}
			remaining := allowed[name][fn.Name.Name]
			opens := rawAllowed[name][fn.Name.Name]
			var scanned body
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					pkg, _ := sel.X.(*ast.Ident)
					if pkg != nil && osNames[pkg.Name] && rawNames[sel.Sel.Name] {
						position := set.Position(sel.Pos())
						scanned.raw = append(scanned.raw, position)
						opens--
						if opens < 0 {
							t.Errorf("%s: open job files with openJobFile", position)
						}
					}
				}
				call, _ := n.(*ast.CallExpr)
				if call == nil {
					return true
				}
				var replace bool
				switch f := call.Fun.(type) {
				case *ast.Ident:
					scanned.calls = append(scanned.calls, f.Name)
					replace = f.Name == "atomicWrite" || f.Name == "atomicWriteSync" ||
						f.Name == "writeJSON" || f.Name == "replaceFile"
				case *ast.SelectorExpr:
					pkg, _ := f.X.(*ast.Ident)
					if pkg != nil && osNames[pkg.Name] {
						replace = f.Sel.Name == "Rename"
					} else {
						scanned.calls = append(scanned.calls, f.Sel.Name)
					}
				}
				if replace {
					remaining--
					if remaining < 0 {
						t.Errorf("%s: unrecognized file replacement in %s", set.Position(call.Pos()), fn.Name)
					}
				}
				return true
			})
			if remaining > 0 || opens > 0 {
				t.Errorf("%s: remove unused storage exceptions for %s", name, fn.Name)
			}
			funcs[fn.Name.Name] = append(funcs[fn.Name.Name], scanned)
		}
	}
	seen := make(map[string]bool)
	var check func(string, string)
	check = func(name, path string) {
		if seen[name] || boundary[name] {
			return
		}
		seen[name] = true
		for _, fn := range funcs[name] {
			for _, pos := range fn.raw {
				t.Errorf("%s: raw job file open reached through %s%s", pos, path, name)
			}
			for _, next := range fn.calls {
				if len(funcs[next]) != 0 {
					check(next, path+name+" -> ")
				}
			}
		}
	}
	for _, entry := range []string{"register", "projectJobStatus", "runNode", "appendJobMailbox",
		"streamLog", "streamMessages", "nodeResult", "Materialize", "Collect", "startNodeJob",
		"serveCodex", "serveClaude", "serveCommand", "cleanNodeJob", "restartNode", "runMuxWorker"} {
		check(entry, "")
	}
}
