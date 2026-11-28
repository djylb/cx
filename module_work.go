package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	exitModuleConflict   = ctlExit(15)
	defaultAcceptTimeout = 20 * time.Minute
	moduleSetupTimeout   = 5 * time.Minute
)

type moduleDefinition struct {
	Name    string     `json:"name"`
	Paths   []string   `json:"paths,omitempty"`
	Accept  [][]string `json:"accept,omitempty"`
	Context []string   `json:"context,omitempty"`
	Deps    []string   `json:"deps,omitempty"`
	Hotspot bool       `json:"hotspot,omitempty"`
}

type moduleTable struct {
	Modules  []moduleDefinition `json:"modules"`
	Hotspots []string           `json:"hotspots"`
}

// The job carries the selected definitions from its dispatch snapshot.
type moduleWork struct {
	ModuleRepository   string             `json:"module_repository,omitempty"`
	ModuleDir          string             `json:"module_dir,omitempty"`
	ModuleOverride     string             `json:"module_override,omitempty"`
	Changeset          string             `json:"changeset,omitempty"`
	ModuleRepositoryID string             `json:"module_repository_id,omitempty"`
	Definitions        []moduleDefinition `json:"module_definitions,omitempty"`
	Hotspots           []string           `json:"hotspots,omitempty"`
	AcceptScripts      []string           `json:"accept_scripts,omitempty"`
	Modules            []string           `json:"modules,omitempty"`
	ContextOutputBytes int64              `json:"context_output_bytes,omitempty"`
	AcceptTimeout      time.Duration      `json:"accept_timeout,omitempty"`
	NoAccept           bool               `json:"no_accept,omitempty"`
	FoldPaths          bool               `json:"fold_paths,omitempty"`
	ReadOnly           bool               `json:"module_read_only,omitempty"`
}

func (w moduleWork) hasModuleWork() bool {
	return len(w.Modules) > 0 || len(w.Hotspots) > 0 || w.Changeset != "" || len(w.AcceptScripts) > 0 || w.NoAccept
}

func (w moduleWork) hasModuleContext() bool {
	return slices.ContainsFunc(w.Definitions, func(m moduleDefinition) bool { return len(m.Context) > 0 })
}

func (o ctlOptions) moduleArguments() []string {
	var args []string
	for _, name := range o.modules {
		args = append(args, "--module", name)
	}
	for _, kv := range [][2]string{{"--module-override", o.moduleOverride}, {"--changeset", o.changeset}, {"--accept-timeout", o.acceptTimeout}} {
		if kv[1] != "" {
			args = append(args, kv[0], kv[1])
		}
	}
	for _, script := range o.accept {
		args = append(args, "--accept", script)
	}
	if o.noAccept {
		args = append(args, "--no-accept")
	}
	return args
}

func (o ctlOptions) validateModuleOptions() error {
	if o.previous != "" && (len(o.modules) != 0 || o.changeset != "" || len(o.accept) != 0 || o.noAccept || o.acceptTimeout != "") {
		return errors.New("module, changeset and acceptance options apply to new jobs")
	}
	if o.command && (len(o.accept) > 0 || o.acceptTimeout != "" || o.noAccept) {
		return errors.New("acceptance options require an agent job")
	}
	if o.moduleOverride != "" && (len(o.modules) == 0 && o.previous == "" || strings.TrimSpace(o.moduleOverride) == "") {
		return errors.New("--module-override requires a reason and at least one --module")
	}
	if strings.TrimSpace(o.changeset) != o.changeset || strings.ContainsAny(o.changeset, "\x00\r\n") {
		return errors.New("--changeset must be a single line without surrounding whitespace")
	}
	if o.acceptTimeout != "" {
		if d, err := time.ParseDuration(o.acceptTimeout); err != nil || d <= 0 {
			return errors.New("--accept-timeout requires a positive duration")
		}
	}
	return nil
}

func foldTargetPaths(platform string) bool {
	osName, _, _ := strings.Cut(platform, "/")
	return osName == "windows" || osName == "darwin"
}

func prepareModuleWork(ctx context.Context, req *jobSubmission, def projectDefinition, o ctlOptions) error {
	work := &req.moduleWork
	work.Changeset, work.ModuleOverride = o.changeset, o.moduleOverride
	work.AcceptScripts, work.NoAccept = o.accept, o.noAccept
	work.ReadOnly = len(o.modules) > 0 && !o.command && o.sandbox == "read-only"
	work.AcceptTimeout, _ = time.ParseDuration(o.acceptTimeout)
	if len(o.modules) == 0 {
		return nil
	}
	if def.ModulesFile != "" && len(def.ModulesCmd) != 0 {
		return errors.New("configure only one of modules_file or modules_cmd")
	}
	if def.ContextOutputBytes < 0 || def.ModulesOutputBytes < 0 {
		return errors.New("module output budgets must be positive")
	}
	work.ContextOutputBytes = def.ContextOutputBytes
	if len(def.ModulesCmd) == 0 && def.ModulesFile == "" {
		if req.Snapshot != "" {
			m, err := loadSnapshot(req.Snapshot)
			if err != nil {
				return err
			}
			for _, repo := range m.Repositories {
				if repo.Path == "." || repo.Path == m.Main || strings.HasPrefix(m.Main, repo.Path+"/") {
					work.ModuleRepository, work.ModuleRepositoryID = repo.Path, repo.RepoID
					break
				}
			}
		} else if req.Source != "" {
			identity, err := projectRepoIdentity(ctx, req.Source)
			if err != nil {
				identity, err = filepath.EvalSymlinks(req.Source)
				if err != nil {
					return err
				}
			}
			work.ModuleRepositoryID = snapSum([]byte(identity))
		}
		for _, name := range o.modules {
			if name != "." && !snapIncludePath(name) || strings.ContainsAny(name, "*?[\r\n") {
				return fmt.Errorf("--module %q must be a repository-relative path prefix", name)
			}
			if !slices.Contains(work.Modules, name) {
				work.Modules = append(work.Modules, name)
				work.Definitions = append(work.Definitions, moduleDefinition{Name: name, Paths: []string{name}})
			}
		}
		return nil
	}
	if req.Snapshot == "" {
		if len(o.modules) > 0 {
			return errors.New("a configured module table requires a snapshot-backed job")
		}
		return nil
	}
	if len(def.ModulesCmd) > 0 && strings.TrimSpace(def.ModulesCmd[0]) == "" {
		return errors.New("modules_cmd requires an executable")
	}
	m, err := loadSnapshot(req.Snapshot)
	if err != nil {
		return err
	}
	repo, err := moduleRepository(ctx, m, req.Sources, def)
	if err != nil {
		return err
	}
	work.ModuleRepository = repo
	for _, member := range m.Repositories {
		if member.Path == repo {
			work.ModuleRepositoryID = member.RepoID
			break
		}
	}
	work.ModuleDir, err = filepath.Rel(filepath.FromSlash(m.Main), filepath.FromSlash(repo))
	if err != nil {
		return err
	}
	work.ModuleDir = filepath.ToSlash(work.ModuleDir)
	table, err := snapshotModuleTable(ctx, m, repo, def)
	if err != nil {
		return err
	}
	work.Hotspots = table.Hotspots
	names := make(map[string]moduleDefinition, len(table.Modules))
	for _, module := range table.Modules {
		if module.Name == "" {
			continue // An unnamed entry cannot be selected by --module.
		}
		if _, err := jobPath("", module.Name); err != nil {
			return fmt.Errorf("invalid modules_cmd name %q", module.Name)
		}
		for _, dep := range module.Deps {
			if _, err := jobPath("", dep); err != nil {
				return fmt.Errorf("invalid dependency %q for module %q", dep, module.Name)
			}
		}
		for _, prefix := range module.Paths {
			if !validModulePattern(prefix) {
				return fmt.Errorf("invalid path %q for module %q", prefix, module.Name)
			}
		}
		key := strings.ToLower(module.Name)
		if _, exists := names[key]; exists {
			return fmt.Errorf("duplicate module %q", module.Name)
		}
		names[key] = module
	}
	for _, pattern := range table.Hotspots {
		if !validModulePattern(pattern) {
			return fmt.Errorf("invalid hotspot path %q", pattern)
		}
	}
	for _, name := range o.modules {
		module, ok := names[strings.ToLower(name)]
		if !ok || module.Name != name {
			return fmt.Errorf("unknown module %q", name)
		}
		if slices.Contains(work.Modules, name) {
			continue
		}
		work.Modules = append(work.Modules, name)
		work.Definitions = append(work.Definitions, module)
		if len(module.Context) > 0 {
			target := strings.ToLower("modules/" + name + ".md")
			for key := range req.Context {
				if modulePrefixesOverlap(strings.ToLower(key), target) {
					return fmt.Errorf("context input %q overlaps generated %s", key, target)
				}
			}
		}
		if module.Hotspot {
			diagnosticf("cx: module %q is a hotspot; apply is serialized for this project\n", name)
		}
	}
	return nil
}

func moduleRepository(ctx context.Context, m Manifest, sources map[string]string, def projectDefinition) (string, error) {
	if def.ModulesRepo != "" {
		var matches []string
		for _, repo := range m.Repositories {
			if repo.Path == def.ModulesRepo || filepath.Base(sources[repo.Path]) == def.ModulesRepo {
				matches = append(matches, repo.Path)
			}
		}
		if len(matches) != 1 {
			return "", fmt.Errorf("modules_repo %q must select one snapshot repository", def.ModulesRepo)
		}
		return matches[0], nil
	}
	if len(def.Repositories) > 0 {
		first, err := projectMemberPath(ctx, def.Repositories[0].Path)
		if err != nil {
			return "", err
		}
		for _, repo := range m.Repositories {
			physical, err := filepath.EvalSymlinks(sources[repo.Path])
			if err == nil && physical == first {
				return repo.Path, nil
			}
		}
	}
	for _, repo := range m.Repositories {
		if repo.Path == m.Main || repo.Path == "." || strings.HasPrefix(m.Main, repo.Path+"/") {
			return repo.Path, nil
		}
	}
	return "", errors.New("module repository is absent from the snapshot")
}

func snapshotModuleTable(ctx context.Context, m Manifest, repo string, def projectDefinition) (moduleTable, error) {
	var table moduleTable
	work, err := os.MkdirTemp("", "cx-modules-")
	if err != nil {
		return table, err
	}
	defer removeTreeBestEffort(work)
	scratch, err := prepareSnapshotInput(ctx, work, m)
	if err != nil {
		return table, err
	}
	tree := filepath.Join(scratch.dir, "tree")
	limit := moduleOutputBudget(def.ModulesOutputBytes)
	cwd := filepath.Join(tree, repo)
	if def.ModulesFile != "" {
		if !snapIncludePath(def.ModulesFile) {
			return table, errors.New("modules_file must name a repository-relative file")
		}
		physical, pathErr := filepath.EvalSymlinks(cwd)
		if pathErr != nil {
			return table, pathErr
		}
		file := filepath.Join(physical, filepath.FromSlash(def.ModulesFile))
		resolved, pathErr := filepath.EvalSymlinks(file)
		if pathErr != nil {
			return table, pathErr
		}
		if resolved != file {
			return table, errors.New("modules_file must be a regular snapshot file without symlinks")
		}
		in, openErr := openJobFile(resolved, jobFileRead)
		if openErr != nil {
			return table, openErr
		}
		defer closeBestEffort(in)
		return decodeModuleTable(in, limit)
	}
	cmd := commandArgv(def.ModulesCmd, false)
	cmd.Dir = cwd
	// The private snapshot already wrote go.work (or chose GOWORK=off).
	cmd.Env = append(snapshotHookEnv(work), scratch.env...)
	stdout, err := createJobTemp(work, "modules-")
	if err != nil {
		return table, err
	}
	defer closeBestEffort(stdout)
	budget := &budgetOutput{dst: stdout, left: limit}
	var stderr commandTail
	cmd.Stdout, cmd.Stderr = budget, &stderr
	_, err = runBoundedCommand(ctx, contextClock(ctx), moduleSetupTimeout, cmd, nil, nil)
	if err != nil {
		return table, fmt.Errorf("modules_cmd in %s: %w (%s)", repo, err, strings.TrimSpace(string(stderr)))
	}
	if budget.truncated {
		return table, fmt.Errorf("modules_cmd output truncated at %d bytes", limit)
	}
	in, err := openJobFile(stdout.Name(), jobFileRead)
	if err != nil {
		return table, err
	}
	defer closeBestEffort(in)
	return decodeModuleTable(in, limit)
}

func decodeModuleTable(in io.Reader, limit int64) (moduleTable, error) {
	var table moduleTable
	bounded := &io.LimitedReader{R: in, N: limit}
	dec := json.NewDecoder(bounded)
	err := dec.Decode(&table)
	if err == nil {
		var extra json.RawMessage
		if trailing := dec.Decode(&extra); trailing != io.EOF {
			err = errors.New("expected one module table")
		}
	}
	if bounded.N == 0 {
		var byteAfterLimit [1]byte
		if n, readErr := in.Read(byteAfterLimit[:]); n != 0 {
			return table, fmt.Errorf("module table exceeds %d bytes", limit)
		} else if readErr != nil && readErr != io.EOF {
			return table, readErr
		}
	}
	if err != nil {
		return table, fmt.Errorf("decode module table: %w", err)
	}
	return table, nil
}

func validModulePattern(pattern string) bool {
	return pattern == "." || snapIncludePattern(pattern) && !strings.ContainsAny(pattern, "\r\n")
}

func modulePrefixesOverlap(a, b string) bool {
	return a == "." || b == "." || a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func modulePatternPrefix(pattern string) string {
	if wildcard := strings.IndexAny(pattern, "*?["); wildcard >= 0 {
		if cut := strings.LastIndexByte(pattern[:wildcard], '/'); cut >= 0 {
			return pattern[:cut]
		}
		return "."
	}
	return pattern
}

func matchesModulePattern(pattern, name string) bool {
	if !strings.ContainsAny(pattern, "*?[") {
		return pattern == "." || name == pattern || strings.HasPrefix(name, pattern+"/")
	}
	parts := strings.Split(name, "/")
	matches := make([]bool, len(parts)+1)
	matches[0] = true
	for segment := range strings.SplitSeq(pattern, "/") {
		if segment == "**" {
			for i := 1; i < len(matches); i++ {
				matches[i] = matches[i] || matches[i-1]
			}
			continue
		}
		for i := len(parts); i > 0; i-- {
			match, _ := path.Match(segment, parts[i-1])
			matches[i] = matches[i-1] && match
		}
		matches[0] = false
	}
	return matches[len(parts)]
}

func (w moduleWork) repoDir(dir string) string {
	return filepath.Join(dir, filepath.FromSlash(w.ModuleDir))
}

func hotspotFiles(names, patterns []string, fold ...bool) []string {
	ignoreCase := slices.Contains(fold, true)
	if ignoreCase {
		patterns = slices.Clone(patterns)
		for i := range patterns {
			patterns[i] = strings.ToLower(patterns[i])
		}
	}
	var matches []string
	for _, name := range names {
		matchName := name
		if ignoreCase {
			matchName = strings.ToLower(name)
		}
		if slices.ContainsFunc(patterns, func(pattern string) bool { return matchesModulePattern(pattern, matchName) }) {
			matches = append(matches, name)
		}
	}
	slices.Sort(matches)
	return matches
}

// hasAcceptance accepts nil: jobs without module work have no checks.
func hasAcceptance(w *moduleWork) bool {
	return w != nil && !w.NoAccept && (len(w.AcceptScripts) > 0 ||
		slices.ContainsFunc(w.Definitions, func(m moduleDefinition) bool { return len(m.Accept) > 0 }))
}

func (w moduleWork) claimRepository() string {
	if w.ModuleRepositoryID != "" {
		return w.ModuleRepositoryID
	}
	return w.ModuleRepository
}
