package main

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

var promptPath = regexp.MustCompile(`(?:^|[\s"'(=])(/[\w.+@-]+(?:/[\w.+@-]+)+)`)

// reportMissingMembers hints at repositories the job seems to need that the
// project does not mount: local replace/use targets of the submitted module
// and absolute paths in the prompt. It only reports; submission continues.
func reportMissingMembers(ctx context.Context, src, prompt string, def projectDefinition) {
	identity := func(dir string) string {
		id, err := projectRepoIdentity(ctx, dir)
		if err != nil {
			return ""
		}
		return id
	}
	members := make(map[string]bool)
	for _, repo := range def.Repositories {
		if id := identity(repo.Path); id != "" {
			members[id] = true
		}
	}
	top, err := projectMemberPath(ctx, src)
	if err != nil {
		return
	}
	var candidates []string
	for _, name := range []string{"go.mod", "go.work"} {
		candidates = append(candidates, localModuleTargets(filepath.Join(top, name))...)
	}
	for _, m := range promptPath.FindAllStringSubmatch(prompt, 64) {
		candidates = append(candidates, m[1])
	}
	var missing []string
	for _, path := range candidates {
		repo, pathErr := projectMemberPath(ctx, existingAncestor(path))
		if pathErr != nil {
			continue
		}
		if id := identity(repo); id != "" && !members[id] && !slices.Contains(missing, repo) {
			missing = append(missing, repo)
		}
	}
	for _, repo := range missing {
		diagnosticf("cx: %s is referenced by the job but is not a member of this project; add it with cx project if the job must change it\n", repo)
	}
}

// localModuleTargets lists filesystem targets of replace (go.mod) and use
// (go.work) directives.
func localModuleTargets(file string) []string {
	in, err := openJobFile(file, jobFileRead)
	if err != nil {
		return nil
	}
	defer closeBestEffort(in)
	var targets []string
	dir := filepath.Dir(file)
	scanner := bufio.NewScanner(in)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if _, after, ok := strings.Cut(line, "=>"); ok {
			line = strings.TrimSpace(after)
		} else if rest, ok := strings.CutPrefix(line, "use "); ok {
			line = strings.TrimSpace(rest)
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		target := strings.Trim(fields[0], `"`)
		if strings.HasPrefix(target, "./") || strings.HasPrefix(target, "../") {
			targets = append(targets, filepath.Join(dir, target))
		} else if filepath.IsAbs(target) {
			targets = append(targets, target)
		}
	}
	return targets
}

func existingAncestor(path string) string {
	for p := filepath.Clean(path); ; p = filepath.Dir(p) {
		if info, err := os.Stat(p); err == nil && info.IsDir() {
			return p
		}
		if p == filepath.Dir(p) {
			return p
		}
	}
}
