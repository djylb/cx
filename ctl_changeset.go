package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// appliedGroup keeps each filter identity apart, so an adopted job's later
// applications report their own session and role.
type appliedGroup struct {
	appliedJob
	Baseline SnapshotID
	Session  string
	Role     string
}

type changesetApplication struct {
	repos   []appliedRepo
	modules []string
}

func readChangesetApplications(changeset, project string) (map[appliedGroup]*changesetApplication, error) {
	path, err := projectOutputPath("changeset")
	if err != nil {
		return nil, err
	}
	var projects []string
	if project != "" {
		projects = []string{project}
	} else {
		entries, readErr := os.ReadDir(filepath.Dir(path))
		if readErr != nil && !os.IsNotExist(readErr) {
			return nil, readErr
		}
		for _, entry := range entries {
			if entry.IsDir() && jobName.MatchString(entry.Name()) {
				projects = append(projects, entry.Name())
			}
		}
	}
	applications := make(map[appliedGroup]*changesetApplication)
	for _, p := range projects {
		err := readApplyLog(p, func(entry applyRecord) {
			if entry.Changeset != changeset {
				return
			}
			key := appliedGroup{entry.appliedJob, entry.Baseline, entry.Session, entry.Role}
			a := applications[key]
			if a == nil {
				a = &changesetApplication{modules: entry.Modules}
				applications[key] = a
			}
			a.repos = append(a.repos, entry.appliedRepo)
		})
		if err != nil {
			return nil, fmt.Errorf("%s apply log: %w", p, err)
		}
	}
	return applications, nil
}

func displayChangeset(ctx context.Context, hs endpoints, o ctlOptions) error {
	var filterHost, filterJob string
	if len(o.pos) > 1 {
		return errors.New("unexpected arguments")
	}
	if len(o.pos) == 1 {
		address := o.pos[0]
		var qualified bool
		filterHost, filterJob, qualified = strings.Cut(address, ":")
		if !qualified {
			filterHost, filterJob = "", address
		}
		if !strings.Contains(filterJob, "/") {
			project, err := defaultProject(ctx)
			if err != nil {
				return err
			}
			filterJob = project + "/" + filterJob
		}
		o.project = applyLogProject(filterJob)
	}
	header := true
	previous := make(map[appliedGroup]string)
	for {
		applications, err := readChangesetApplications(o.changeset, o.project)
		if err != nil {
			return err
		}
		nodes := make(map[appliedJob]fields)
		ch := make(chan hostReply, len(hs))
		count := 0
		for host, h := range hs {
			if filterHost != "" && filterHost != host {
				continue
			}
			count++
			go func() {
				observing, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				r, e := h.call(observing, 5*time.Second, "rows", o.project, "")
				ch <- hostReply{name: host, r: r, e: e}
			}()
		}
		for range count {
			v := <-ch
			var rows []fields
			if v.e != nil || v.r.code != 0 || decodeRows(v.r.data, &rows) != nil {
				continue
			}
			for _, row := range rows {
				run, _ := row["run_id"].(string)
				nodes[appliedJob{Host: v.name, Job: listingJobID(row), RunID: run}] = row
			}
		}
		groups := slices.SortedFunc(maps.Keys(applications), func(a, b appliedGroup) int {
			return strings.Compare(a.Host+":"+a.Job+":"+a.RunID+":"+string(a.Baseline)+":"+a.Session+":"+a.Role, b.Host+":"+b.Job+":"+b.RunID+":"+string(b.Baseline)+":"+b.Session+":"+b.Role)
		})
		var out bytes.Buffer
		enc := json.NewEncoder(&out)
		for _, group := range groups {
			a := applications[group]
			if filterJob != "" && group.Job != filterJob || filterHost != "" && group.Host != filterHost {
				continue
			}
			p, j, _ := strings.Cut(group.Job, "/")
			row := fields{"host": group.Host, "project": p, "job": j, "run_id": group.RunID, "baseline": group.Baseline,
				"changeset": o.changeset, "session": group.Session, "modules": a.modules, "role": group.Role, "status": "applied"}
			if node := nodes[group.appliedJob]; node != nil {
				row["node"] = node
			}
			if o.session != "" && row["session"] != o.session || o.role != "" && group.Role != o.role || !matchesModuleListing(row, o) {
				continue
			}
			row["applications"] = a.repos
			if o.watch {
				packed, err := json.Marshal(row)
				if err != nil {
					return err
				}
				if previous[group] == string(packed) {
					continue
				}
				previous[group] = string(packed)
			}
			if o.json {
				if err := enc.Encode(row); err != nil {
					return err
				}
			} else {
				renderLSRow(&out, &header, o.brief, group.Job, group.Host, row["role"], fmt.Sprint(row["status"]), row)
			}
		}
		if err := preview(out.Bytes(), o); err != nil {
			return err
		}
		if !o.watch {
			return nil
		}
		if err := pause(ctx, o.interval); err != nil {
			return err
		}
	}
}
