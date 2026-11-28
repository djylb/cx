package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const readsFilename = "reads.json"

type turnMetadata struct {
	CumulativeUsage  *usageCounters    `json:"cumulative_usage"`
	Usage            *usageSummary     `json:"usage"`
	ContextWarning   *contextWarning   `json:"context_warning,omitempty"`
	AcceptanceStatus string            `json:"acceptance_status"`
	ContextVersion   string            `json:"context_version,omitempty"`
	Reads            []readObservation `json:"reads"`
}

func turnMetadataPath(dir, digest string) string {
	return filepath.Join(dir, "turns", digest+".json")
}

func (r *runner) publishAgentTurn(t turn, report []byte, usage *usageSummary, warning *contextWarning, live bool) error {
	if previous, err := findTurnReport(r.dir, t.ID); err != nil || previous.Turn != "" {
		return err
	}
	metadata := turnMetadata{Usage: usage, ContextWarning: warning, AcceptanceStatus: acceptanceDisposition(r.c.Work, t.Status, live), ContextVersion: r.moduleContextVersion, Reads: []readObservation{}}
	if latest := readUsage(r.s.Usage); latest != nil {
		metadata.CumulativeUsage = latest.Total
	}
	if r.reads.turn == t.ID {
		metadata.Reads = append(metadata.Reads, r.reads.paths...)
	}
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	digest := snapSum(data)
	if err = publishImmutable(turnMetadataPath(r.dir, digest), data, r.dir); err != nil {
		_ = r.note("turn/metadata-failed", err.Error())
		digest = ""
	}
	return r.publishTurnReportMetadata(t.ID, report, digest)
}

func readTurnMetadata(dir, id string) (turnMetadata, error) {
	ref, err := findTurnReport(dir, id)
	if err != nil || ref.Metadata == "" {
		return turnMetadata{}, err
	}
	if !snapHex(ref.Metadata, 64) {
		return turnMetadata{}, fmt.Errorf("invalid metadata reference for turn %q", id)
	}
	data, err := readSharedFile(turnMetadataPath(dir, ref.Metadata))
	if err != nil {
		return turnMetadata{}, err
	}
	if snapSum(data) != ref.Metadata {
		return turnMetadata{}, fmt.Errorf("invalid metadata content for turn %q", id)
	}
	var details turnMetadata
	err = json.Unmarshal(data, &details)
	return details, err
}

func stageTurnMetadata(dir, stage string, meta *turnResult) error {
	details, err := readTurnMetadata(dir, meta.TurnID)
	if err != nil {
		return nil // Missing optional observations do not invalidate delivery.
	}
	meta.CumulativeUsage = details.CumulativeUsage
	meta.Usage, meta.ContextWarning, meta.ContextVersion = details.Usage, details.ContextWarning, details.ContextVersion
	if meta.ContextWarning != nil {
		meta.ContextWarning.Turn = meta.Turn
	}
	if details.AcceptanceStatus != "" {
		meta.AcceptanceStatus = details.AcceptanceStatus
	}
	if details.Reads == nil {
		return nil
	}
	// The read log is an optional attachment: failing to write it leaves the
	// turn deliverable and the reads unavailable.
	reads, err := json.Marshal(turnReads{Turn: meta.Turn, Status: "available", Paths: details.Reads})
	if err == nil {
		err = writeJobBytes(filepath.Join(stage, readsFilename), append(reads, '\n'))
	}
	if err != nil {
		diagnosticf("cx: read log for turn %d unavailable: %v\n", meta.Turn, err)
		return nil
	}
	meta.ReadsPath = readsFilename
	return nil
}

func nodeReadsHistory(ctx context.Context, id, run string) error {
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
	if identity.run != run {
		return errJobReplaced
	}
	work, err := workDir(dir, s)
	if err != nil {
		return err
	}
	records, err := completionCheckpoints(dir)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return err
		}
		var turn turnCompletion
		if err := json.Unmarshal(record.Value, &turn); err != nil {
			return err
		}
		if turn.RunID != run || turn.Number > s.Finished {
			continue
		}
		folder := filepath.Join(work, "result", fmt.Sprintf("turn-%d", turn.Number))
		if turn.Number == s.Finished {
			if _, err := sealTurn(ctx, dir, s, turn.Number); err != nil && !errors.Is(err, ctlExit(44)) {
				return err
			}
		}
		if err := identity.checkDir(dir); err != nil {
			return err
		}
		reads, _ := readResultReads(folder, turn.Number)
		if err := enc.Encode(reads); err != nil {
			return err
		}
	}
	return nil
}

func acceptanceDisposition(w *moduleWork, outcome string, live bool) string {
	if !hasAcceptance(w) || !live || outcome != "completed" && outcome != "failed" {
		return "none"
	}
	return "pending"
}
