package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

type claimAddress struct {
	Host  string `json:"host"`
	Job   string `json:"job"`
	RunID string `json:"run_id,omitempty"`
}

func (a claimAddress) key() string { return a.Host + ":" + a.Job }

func (a claimAddress) project() string {
	p, _, ok := strings.Cut(a.Job, "/")
	if !ok {
		return ""
	}
	return p
}

type claimObservation struct {
	moduleListing
	Project string `json:"project"`
	Job     string `json:"job"`
	State   string `json:"status"`
	RunID   string `json:"run_id"`
}

func observeClaims(ctx context.Context, hs endpoints, project string) (map[string]claimObservation, []string) {
	names := slices.Sorted(maps.Keys(hs))
	views := make([][]claimObservation, len(names))
	failed := make([]bool, len(names))
	var queries sync.WaitGroup
	for i, host := range names {
		queries.Go(func() {
			bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			response, err := hs[host].call(bounded, 30*time.Second, "rows", project, "")
			if err == nil {
				if response.code != 0 {
					err = ctlExit(response.code)
				} else {
					err = json.Unmarshal(response.data, &views[i])
					if err == nil && views[i] == nil {
						err = errors.New("expected a job listing")
					}
				}
			}
			failed[i] = err != nil
			if err != nil {
				diagnosticf("cx: could not check module claims on %s (%v)\n", host, err)
			}
		})
	}
	queries.Wait()
	var unchecked []string
	observed := make(map[string]claimObservation)
	for i, host := range names {
		if failed[i] {
			unchecked = append(unchecked, host)
			continue
		}
		for _, row := range views[i] {
			if row.Project == project {
				observed[host+":"+project+"/"+row.Job] = row
			}
		}
	}
	return observed, unchecked
}

func activeClaimState(state string) bool {
	return state != "" && state != "closed" && state != "submit_failed" && !isTerminal(state)
}

// Claims are point-in-time comparisons of existing node job inputs.
func checkModuleClaims(ctx context.Context, hs endpoints, target claimAddress, source *claimAddress, w moduleWork) ([]string, error) {
	if w.ModuleOverride != "" && strings.TrimSpace(w.ModuleOverride) == "" {
		return nil, errors.New("--module-override requires a reason")
	}
	if target.project() == "" || !w.writableClaim() {
		return nil, nil
	}
	rows, unchecked := observeClaims(ctx, hs, target.project())
	if err := ctx.Err(); err != nil {
		return unchecked, err
	}
	var conflicts, dependencies []string
	for _, key := range slices.Sorted(maps.Keys(rows)) {
		row := rows[key]
		if !activeClaimState(row.State) {
			continue
		}
		if source != nil && key == source.key() && (source.RunID == "" || row.RunID == source.RunID) {
			continue
		}
		if key == target.key() && (source == nil || source.key() != target.key()) {
			continue
		}
		host, _, _ := strings.Cut(key, ":")
		fold := w.FoldPaths || row.FoldPaths || foldTargetPaths(hs[host].Platform)
		overlap, dependency := moduleClaimRelation(w, row.ModuleRepository, row.ModuleClaims, fold)
		for _, name := range dependency {
			dependencies = append(dependencies, fmt.Sprintf("%s (%s)", key, name))
		}
		for _, name := range overlap {
			conflicts = append(conflicts, fmt.Sprintf("%s (%s)", key, name))
		}
	}
	for _, dependency := range dependencies {
		diagnosticf("cx: direct dependencies have an active writer at %s\n", dependency)
	}
	reportUncheckedClaims(unchecked)
	return unchecked, reportModuleConflicts(w.ModuleOverride, conflicts)
}

type jobClaimInfo struct {
	RunID      string      `json:"run_id"`
	Directory  string      `json:"directory"`
	Baseline   string      `json:"baseline"`
	ActiveOnly bool        `json:"active_only,omitempty"`
	Override   string      `json:"override,omitempty"`
	State      string      `json:"status"`
	Work       *moduleWork `json:"work,omitempty"`
}

func nodeClaimInfo(ctx context.Context, id string) error {
	root, err := stateRoot()
	if err != nil {
		return err
	}
	dir, err := jobDir(root, id)
	if err != nil {
		return err
	}
	identity, s, _, err := captureJobIdentity(dir)
	if err != nil {
		return err
	}
	if _, err = projectJobStatus(ctx, dir, &s, identity); err != nil {
		return err
	}
	var c config
	if err = readJobConfig(dir, &c); err != nil {
		return err
	}
	work, err := workDir(dir, s)
	if err != nil {
		return err
	}
	base, err := readJobBaseline(work)
	if err != nil {
		return err
	}
	if base.Work != nil {
		c.Work = base.Work
	}
	digest, err := claimBaseline(base)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(jobClaimInfo{RunID: s.RunID, Directory: identity.key, Baseline: digest, State: s.State, Work: c.Work})
}

func getJobClaimInfo(ctx context.Context, h endpoint, id string) (jobClaimInfo, error) {
	var info jobClaimInfo
	response, err := h.call(ctx, 15*time.Second, "claim-info", id)
	if err != nil {
		return info, err
	}
	if response.code != 0 {
		return info, response.output()
	}
	err = json.Unmarshal(response.data, &info)
	return info, err
}

func (w moduleWork) writableClaim() bool { return len(w.Modules) != 0 && !w.ReadOnly }

func claimBaseline(base jobBaseline) (string, error) {
	data, err := json.Marshal(base)
	return snapSum(data), err
}

func (info jobClaimInfo) check(dir string, s status) error {
	if err := (jobIdentity{run: info.RunID, key: info.Directory}).check(dir, s); err != nil {
		return err
	}
	work, err := workDir(dir, s)
	if err != nil {
		return err
	}
	base, err := readJobBaseline(work)
	if err != nil {
		return err
	}
	digest, err := claimBaseline(base)
	if err == nil && digest != info.Baseline {
		return errors.New("effective baseline changed before restart; retry the command")
	}
	return err
}

func (info jobClaimInfo) wire() string {
	info.Work = nil
	data, _ := json.Marshal(info)
	return string(data)
}

func withRestartClaim(ctx context.Context, hs endpoints, h endpoint, source, target claimAddress, override string, dispatch func(jobClaimInfo) (reply, error)) (reply, error) {
	info, err := getJobClaimInfo(ctx, h, source.Job)
	if err != nil {
		return reply{}, err
	}
	if source.key() != target.key() && activeClaimState(info.State) {
		reportActiveSource(source)
	}
	info.ActiveOnly = source == target && activeClaimState(info.State)
	info.Override = override
	if info.Work != nil && !info.ActiveOnly {
		work := *info.Work
		work.ModuleOverride = override
		if source == target {
			source.RunID = info.RunID
		}
		if _, err := checkModuleClaims(ctx, hs, target, &source, work); err != nil {
			return reply{}, err
		}
	}
	return dispatch(info)
}

func reportActiveSource(source claimAddress) {
	diagnosticf("cx: source %s is still active; consider cx close %s before starting its successor\n", source.key(), source.key())
}

func reportUncheckedClaims(unchecked []string) {
	if len(unchecked) != 0 {
		diagnosticf("cx: claims_unchecked: %s\n", strings.Join(unchecked, ", "))
	}
}
