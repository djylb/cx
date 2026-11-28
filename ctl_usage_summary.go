package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
)

type usageSamples struct {
	Total   int `json:"total_tokens"`
	Cached  int `json:"cached_tokens"`
	Fresh   int `json:"fresh_input_tokens"`
	Output  int `json:"output_tokens"`
	Context int `json:"context_percent"`
}

type usageRank struct {
	Samples int          `json:"samples"`
	Known   usageSamples `json:"known_samples"`
	Name    string       `json:"name"`
	usageCounters
	CompletedTurns int64    `json:"completed_turns,omitempty"`
	ContextPercent *float64 `json:"context_percent,omitempty"`
	TurnContext    *float64 `json:"turn_context_percent,omitempty"`
	HighContext    bool     `json:"high_context,omitempty"`
	Stale          bool     `json:"stale,omitempty"`
}

type usageRanking struct {
	Projects        []usageRank `json:"projects"`
	Jobs            []usageRank `json:"jobs"`
	Roles           []usageRank `json:"roles"`
	HighContextJobs []usageRank `json:"high_context_jobs"`
}

type usageBoard struct {
	projects, roles map[string]usageRank
	jobs            []usageRank
	settings        usageSettings
}

func usageListSettings(o ctlOptions) (usageSettings, error) {
	settings, err := loadUsageSettings()
	if err != nil {
		return settings, err
	}
	for _, opt := range []struct {
		value, flag string
		field       *int
	}{
		{o.usageTop, "--usage-top", &settings.Top},
		{o.usageTurns, "--usage-turns", &settings.MinTurns},
	} {
		if opt.value == "" {
			continue
		}
		n, parseErr := strconv.Atoi(opt.value)
		if parseErr != nil || n < 1 {
			return settings, fmt.Errorf("%s needs a positive integer", opt.flag)
		}
		*opt.field = n
	}
	if o.usageContext != "" {
		settings.MinContext, err = usagePercent(o.usageContext)
	}
	return settings, err
}

func newUsageBoard(settings usageSettings) *usageBoard {
	return &usageBoard{settings: settings, projects: make(map[string]usageRank), roles: make(map[string]usageRank)}
}

func rowUsageCount(raw any) *int64 {
	switch v := raw.(type) {
	case json.Number:
		if count, err := v.Int64(); err == nil {
			return &count
		}
	case *int64:
		return v
	}
	return nil
}

func rowUsagePercent(raw any) *float64 {
	switch v := raw.(type) {
	case json.Number:
		if percent, err := v.Float64(); err == nil {
			return &percent
		}
	case *float64:
		return v
	}
	return nil
}

func (b *usageBoard) add(host, project, id, role string, row fields) {
	entry := usageRank{
		Name: host + ":" + id,
		usageCounters: usageCounters{Total: rowUsageCount(row["tokens"]), Cached: rowUsageCount(row["cached_tokens"]),
			Fresh: rowUsageCount(row["fresh_input_tokens"]), Output: rowUsageCount(row["output_tokens"])},
		ContextPercent: rowUsagePercent(row["context_percent"]), TurnContext: rowUsagePercent(row["turn_context_percent"]), Stale: row["stale"] == true,
	}
	if turns := rowUsageCount(row["completed_turns"]); turns != nil {
		entry.CompletedTurns = *turns
	}
	// Only a sample owned by the last completed turn counts toward high context.
	entry.HighContext = entry.CompletedTurns >= int64(b.settings.MinTurns) && entry.TurnContext != nil && *entry.TurnContext >= b.settings.MinContext
	entry.Samples = 1
	for _, metric := range []struct {
		known bool
		count *int
	}{
		{entry.Total != nil, &entry.Known.Total}, {entry.Cached != nil, &entry.Known.Cached},
		{entry.Fresh != nil, &entry.Known.Fresh}, {entry.Output != nil, &entry.Known.Output},
		{entry.ContextPercent != nil, &entry.Known.Context},
	} {
		if metric.known {
			*metric.count = 1
		}
	}
	if entry.Known != (usageSamples{}) {
		b.jobs = append(b.jobs, entry)
	}
	if project == "" {
		project = "(none)"
	}
	if role == "" {
		role = "worker"
	}
	accumulateUsage(b.projects, project, entry)
	accumulateUsage(b.roles, role, entry)
}

func accumulateUsage(groups map[string]usageRank, name string, entry usageRank) {
	s := groups[name]
	s.Name = name
	sum := func(a, b *int64) *int64 {
		if a == nil {
			return b
		}
		if b == nil {
			return a
		}
		value := *a + *b
		return &value
	}
	s.Total, s.Cached, s.Fresh, s.Output = sum(s.Total, entry.Total), sum(s.Cached, entry.Cached), sum(s.Fresh, entry.Fresh), sum(s.Output, entry.Output)
	s.Samples += entry.Samples
	s.Known.Total += entry.Known.Total
	s.Known.Cached += entry.Known.Cached
	s.Known.Fresh += entry.Known.Fresh
	s.Known.Output += entry.Known.Output
	s.Known.Context += entry.Known.Context
	s.Stale = s.Stale || entry.Stale
	groups[name] = s
}

func usageCompare(a, b usageRank) int {
	count := func(n *int64) int64 {
		if n == nil {
			return -1
		}
		return *n
	}
	left, right := count(a.Total), count(b.Total)
	switch {
	case left > right:
		return -1
	case left < right:
		return 1
	default:
		return strings.Compare(a.Name, b.Name)
	}
}

func (b *usageBoard) report() usageRanking {
	projects := slices.SortedFunc(maps.Values(b.projects), usageCompare)
	roles := slices.SortedFunc(maps.Values(b.roles), usageCompare)
	unknown := func(row usageRank) bool { return row.Known == (usageSamples{}) }
	projects = slices.DeleteFunc(projects, unknown)
	roles = slices.DeleteFunc(roles, unknown)
	slices.SortFunc(b.jobs, usageCompare)
	report := usageRanking{
		Projects:        append([]usageRank{}, projects[:min(len(projects), b.settings.Top)]...),
		Roles:           append([]usageRank{}, roles[:min(len(roles), b.settings.Top)]...),
		Jobs:            append([]usageRank{}, b.jobs[:min(len(b.jobs), b.settings.Top)]...),
		HighContextJobs: []usageRank{},
	}
	for _, row := range b.jobs {
		if row.HighContext {
			report.HighContextJobs = append(report.HighContextJobs, row)
		}
	}
	return report
}

func (b *usageBoard) write(out io.Writer, jsonMode bool) error {
	report := b.report()
	if jsonMode {
		return json.NewEncoder(out).Encode(report)
	}
	for _, section := range []struct {
		title string
		rows  []usageRank
	}{
		{"Projects", report.Projects}, {"Jobs", report.Jobs}, {"Roles", report.Roles}, {"Multi-turn jobs with high context", report.HighContextJobs},
	} {
		if _, err := fmt.Fprintln(out, section.title); err != nil {
			return err
		}
		for _, row := range section.rows {
			label := row.Name
			if row.HighContext {
				label += fmt.Sprintf(" [%d turns, %.1f%% context]", row.CompletedTurns, *row.ContextPercent)
			}
			if row.Stale {
				label += " [cached]"
			}
			label += fmt.Sprintf(" [known of %d: cached %d, fresh %d, output %d, total %d, context %d]", row.Samples, row.Known.Cached, row.Known.Fresh, row.Known.Output, row.Known.Total, row.Known.Context)
			if err := writeUsageCounters(out, label, &row.usageCounters); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateUsageOptions(o ctlOptions) error {
	if !o.usage && (o.usageTop != "" || o.usageTurns != "" || o.usageContext != "") {
		return errors.New("usage ranking options require --usage")
	}
	if o.usage && o.changeset != "" {
		return errors.New("--usage and --changeset select different listing views")
	}
	return nil
}
