package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

const defaultContextWarn = 60

type usageSettings struct {
	ContextWarn float64 `toml:"context_warn"`
	Top         int     `toml:"usage_top"`
	MinTurns    int     `toml:"usage_turns"`
	MinContext  float64 `toml:"usage_context"`
}

func loadUsageSettings() (usageSettings, error) {
	s := usageSettings{ContextWarn: defaultContextWarn, Top: 10, MinTurns: 4, MinContext: 50}
	dir, err := projectDirectory()
	if err != nil {
		return s, err
	}
	_, err = toml.DecodeFile(filepath.Join(filepath.Dir(dir), "usage.toml"), &s)
	if err != nil && !os.IsNotExist(err) {
		return s, err
	}
	if !validPercent(s.ContextWarn) || !validPercent(s.MinContext) || s.Top < 1 || s.MinTurns < 1 {
		return s, errors.New("invalid usage settings")
	}
	return s, nil
}

func validPercent(v float64) bool { return v >= 0 && v <= 100 }

func usagePercent(value string) (float64, error) {
	v, err := strconv.ParseFloat(value, 64)
	if err != nil || !validPercent(v) {
		return 0, fmt.Errorf("invalid percentage %q (expected 0–100)", value)
	}
	return v, nil
}

type usageCounters struct {
	Total  *int64 `json:"total_tokens"`
	Cached *int64 `json:"cached_tokens"`
	Fresh  *int64 `json:"fresh_input_tokens"`
	Output *int64 `json:"output_tokens"`
}

type rawUsageCounters struct {
	TotalTokens       *int64 `json:"totalTokens,omitempty"`
	InputTokens       *int64 `json:"inputTokens,omitempty"`
	CachedInputTokens *int64 `json:"cachedInputTokens,omitempty"`
	OutputTokens      *int64 `json:"outputTokens,omitempty"`
}

func (u *rawUsageCounters) counters() *usageCounters {
	if u == nil {
		return nil
	}
	out := &usageCounters{Total: u.TotalTokens, Cached: u.CachedInputTokens, Output: u.OutputTokens}
	if u.InputTokens != nil && u.CachedInputTokens != nil {
		fresh := max(0, *u.InputTokens-*u.CachedInputTokens)
		out.Fresh = &fresh
	}
	if out.Total == nil && u.InputTokens != nil && out.Output != nil {
		total := *u.InputTokens + *out.Output
		out.Total = &total
	}
	return out
}

type usageSummary struct {
	Total          *usageCounters `json:"total,omitempty"`
	Turn           *usageCounters `json:"turn,omitempty"`
	ContextPercent *float64       `json:"context_percent"`
}

type engineUsage struct {
	Last               *rawUsageCounters `json:"last,omitempty"`
	Total              *rawUsageCounters `json:"total,omitempty"`
	ModelContextWindow float64           `json:"modelContextWindow,omitempty"`
}

func retainEngineUsage(current, previous json.RawMessage, sameTurn bool) (json.RawMessage, bool) {
	var next, prior engineUsage
	if json.Unmarshal(current, &next) != nil || next.Last == nil && next.Total == nil {
		return previous, false
	}
	hasSample := next.Last != nil
	if json.Unmarshal(previous, &prior) == nil {
		if next.Total == nil {
			next.Total = prior.Total
		} else if prior.Total != nil {
			fill := func(field **int64, value *int64) {
				if *field == nil {
					*field = value
				}
			}
			fill(&next.Total.TotalTokens, prior.Total.TotalTokens)
			fill(&next.Total.InputTokens, prior.Total.InputTokens)
			fill(&next.Total.CachedInputTokens, prior.Total.CachedInputTokens)
			fill(&next.Total.OutputTokens, prior.Total.OutputTokens)
		}
		if sameTurn && next.Last == nil {
			next.Last = prior.Last
			next.ModelContextWindow = prior.ModelContextWindow
		}
	}
	data, err := json.Marshal(next)
	if err != nil {
		return previous, false
	}
	return data, hasSample
}

func readUsage(raw json.RawMessage) *usageSummary {
	var input engineUsage
	if len(raw) == 0 || json.Unmarshal(raw, &input) != nil || input.Last == nil && input.Total == nil {
		return nil
	}
	m := &usageSummary{Total: input.Total.counters(), Turn: input.Last.counters()}
	if input.ModelContextWindow > 0 && m.Turn != nil && m.Turn.Total != nil {
		percent := 100 * float64(*m.Turn.Total) / input.ModelContextWindow
		m.ContextPercent = &percent
	}
	return m
}

func usage(row fields, raw json.RawMessage) {
	row["tokens"], row["context_percent"] = nil, nil
	row["cached_tokens"], row["fresh_input_tokens"], row["output_tokens"] = nil, nil, nil
	m := readUsage(raw)
	if m != nil {
		row["context_percent"] = m.ContextPercent
		if m.Total != nil {
			row["tokens"], row["cached_tokens"], row["fresh_input_tokens"], row["output_tokens"] =
				m.Total.Total, m.Total.Cached, m.Total.Fresh, m.Total.Output
		}
	}
}

type contextWarning struct {
	Turn      int64    `json:"turn"`
	Percent   float64  `json:"context_percent"`
	Threshold float64  `json:"threshold"`
	Actions   []string `json:"actions"`
}

func (r *runner) usageWarning(usage *usageSummary) *contextWarning {
	threshold := float64(defaultContextWarn)
	if r.c.ContextWarn != nil {
		threshold = *r.c.ContextWarn
	}
	if threshold == 0 || usage == nil || usage.ContextPercent == nil || *usage.ContextPercent < threshold {
		return nil
	}
	return &contextWarning{Turn: r.s.Finished + 1, Percent: *usage.ContextPercent, Threshold: threshold, Actions: []string{"continue", "compact", "handoff"}}
}

func (w contextWarning) text() string {
	return fmt.Sprintf("Context is %.1f%% (threshold %g%%). Continue. Compact in place and continue in the same job (recommended to avoid rereading). Write a handoff summary and start a new job only when the task changes.", w.Percent, w.Threshold)
}

func (r *runner) notifyContextWarning(warning *contextWarning) {
	if warning == nil {
		return
	}
	identity, _, _, err := captureJobIdentity(r.dir)
	if err == nil {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.baseContext()), controlCallIdle)
		defer cancel()
		err = publishTurnEvent(ctx, r.dir, identity, warning.Turn, &agentMail{Type: "context_warning", To: "parent", Text: warning.text(), ContextWarning: warning})
	}
	if err != nil {
		_ = r.note("usage/context-warning-error", err.Error())
	}
}

func writeUsageCounters(out io.Writer, label string, counters *usageCounters) error {
	if counters == nil {
		return nil
	}
	format := func(v *int64) string {
		if v == nil {
			return "-"
		}
		return strconv.FormatInt(*v, 10)
	}
	_, err := fmt.Fprintf(out, "%s  cached %s  fresh input %s  output %s  total %s\n",
		strings.TrimSpace(label), format(counters.Cached), format(counters.Fresh), format(counters.Output), format(counters.Total))
	return err
}
