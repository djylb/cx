package main

import (
	"slices"
	"strings"
)

type moduleClaim struct {
	Name  string   `json:"name"`
	Paths []string `json:"paths,omitempty"`
}

type moduleListing struct {
	FoldPaths        bool          `json:"module_fold_paths,omitempty"`
	Modules          []string      `json:"modules,omitempty"`
	ModuleClaims     []moduleClaim `json:"module_claims,omitempty"`
	ModuleRepository string        `json:"module_repo,omitempty"`
	ModuleOverride   string        `json:"module_override,omitempty"`
	Changeset        string        `json:"changeset,omitempty"`
}

func (w moduleWork) listing() moduleListing {
	m := moduleListing{Modules: w.Modules, ModuleOverride: w.ModuleOverride, Changeset: w.Changeset}
	if !w.ReadOnly && len(w.Modules) != 0 {
		m.ModuleRepository = w.claimRepository()
		m.FoldPaths = w.FoldPaths
		m.ModuleClaims = make([]moduleClaim, 0, len(w.Definitions))
		for _, d := range w.Definitions {
			m.ModuleClaims = append(m.ModuleClaims, moduleClaim{Name: d.Name, Paths: d.Paths})
		}
	}
	return m
}

func (m moduleListing) addModuleListing(row fields) {
	if len(m.Modules) != 0 {
		row["modules"] = m.Modules
	}
	if len(m.ModuleClaims) != 0 {
		row["module_claims"] = m.ModuleClaims
		row["module_repo"] = m.ModuleRepository
		row["module_fold_paths"] = m.FoldPaths
	}
	if m.Changeset != "" {
		row["changeset"] = m.Changeset
	}
	if m.ModuleOverride != "" {
		row["module_override"] = m.ModuleOverride
	}
}

func listingModules(row fields) []string {
	switch names := row["modules"].(type) {
	case []string:
		return names
	case []any:
		var modules []string
		for _, name := range names {
			if value, ok := name.(string); ok {
				modules = append(modules, value)
			}
		}
		return modules
	default:
		return nil
	}
}

func matchesModuleListing(row fields, o ctlOptions) bool {
	names := listingModules(row)
	return (o.changeset == "" || row["changeset"] == o.changeset) &&
		(len(o.modules) == 0 || slices.ContainsFunc(o.modules, func(name string) bool { return slices.Contains(names, name) }))
}

func moduleClaimRelation(w moduleWork, repo string, old []moduleClaim, fold ...bool) (overlap, dependency []string) {
	ignoreCase := slices.Contains(fold, true)
	currentRepo := w.claimRepository()
	// An unknown repository identity is treated as the same repository: the check stays conservative.
	equalRepo := repo == "" || currentRepo == "" || repo == currentRepo || ignoreCase && strings.EqualFold(repo, currentRepo)
	for _, current := range w.Definitions {
		for _, other := range old {
			if current.Name == other.Name {
				overlap = append(overlap, current.Name)
			}
			if slices.Contains(current.Deps, other.Name) {
				dependency = append(dependency, current.Name+" -> "+other.Name)
			}
			if current.Name != other.Name && equalRepo && modulePathsOverlap(current.Paths, other.Paths, ignoreCase) {
				overlap = append(overlap, current.Name+" / "+other.Name)
			}
		}
	}
	return overlap, dependency
}

func modulePathsOverlap(a, b []string, fold bool) bool {
	for _, left := range a {
		for _, right := range b {
			l, r := modulePatternPrefix(left), modulePatternPrefix(right)
			if fold {
				l, r = strings.ToLower(l), strings.ToLower(r)
			}
			if modulePrefixesOverlap(l, r) {
				return true
			}
		}
	}
	return false
}

func reportModuleConflicts(override string, conflicts []string) error {
	if len(conflicts) == 0 {
		return nil
	}
	if override != "" {
		diagnosticf("cx: overriding module claims held by %s (%q)\n", strings.Join(conflicts, ", "), override)
		return nil
	}
	diagnosticf("STATUS: BLOCKED\nModule claims held by %s. Use --module-override REASON to continue.\n", strings.Join(conflicts, ", "))
	return exitModuleConflict
}
