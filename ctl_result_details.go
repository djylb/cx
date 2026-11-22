package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ResultDetails records the engine state and log of a sealed turn.
type ResultDetails struct {
	Status   *string `json:"status"`
	ExitCode *int64  `json:"exit_code"`
	Engine   string  `json:"engine"`
	Log      string  `json:"log,omitempty"`
}

// The detailed archive includes the same sealed turn plus a log. Its separate
// name keeps previously advertised byte offsets valid for both variants.
func resultVariant(path string, details bool) string {
	if details {
		return path + ".details"
	}
	return path
}

func resultDetailsArg(a []string) (bool, error) {
	switch {
	case len(a) == 4:
		return false, nil
	case len(a) == 5 && a[4] == "details":
		return true, nil
	default:
		return false, errors.New("invalid result transfer arguments")
	}
}

func stageResultDetails(ctx context.Context, dir, folder, extra string, s status, progress io.Writer) error {
	var m turnResult
	if err := readJSON(filepath.Join(folder, "result.json"), &m); err != nil {
		return err
	}
	latest := s.Finished == m.Turn && readyResult(s)
	log, err := copyResultLog(ctx, dir, extra, s.logName(), m.TurnID, latest, progress)
	if err != nil {
		return err
	}
	m.ResultDetails = &ResultDetails{Engine: s.Engine, Log: log}
	if latest {
		var after status
		if err = loadJobStatusInto(dir, &after); err != nil {
			return err
		}
		if s.RunID != after.RunID || s.Finished != after.Finished || s.Turn != after.Turn || !readyResult(after) {
			return fmt.Errorf("%w: job advanced while collecting result log", ctlExit(44))
		}
		m.Status, m.ExitCode = &after.State, after.ExitCode
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return snapImmutable(filepath.Join(extra, "result.json"), data)
}

func copyResultLog(ctx context.Context, dir, stage, name, turnID string, latest bool, progress io.Writer) (string, error) {
	if (name == "output.log" || turnID == "") && !latest {
		return "", nil
	}
	in, err := openJobFile(filepath.Join(dir, name), jobFileRead)
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	defer closeBestEffort(in)
	fi, err := in.Stat()
	if err != nil {
		return "", err
	}
	start, end := int64(0), fi.Size()
	if name == "progress.log" && turnID != "" {
		var found bool
		start, end, found, err = turnLogRange(ctx, io.TeeReader(in, progress), turnID, end)
		if err != nil || (!found && !latest) {
			return "", err
		}
	}
	out, err := openJobFile(filepath.Join(stage, name), jobFileNew)
	if err != nil {
		return "", err
	}
	copied, err := io.Copy(io.MultiWriter(out, progress), io.NewSectionReader(in, start, end-start))
	if err = errors.Join(err, out.Close()); err != nil {
		return "", err
	}
	if copied != end-start {
		return "", errors.New("log changed while collecting result")
	}
	return name, nil
}

func turnLogRange(ctx context.Context, in io.Reader, id string, size int64) (start, end int64, found bool, err error) {
	start = -1
	previousEnd := int64(0)
	reader := bufio.NewReader(io.LimitReader(in, size))
	for offset := int64(0); offset < size; {
		if err = ctx.Err(); err != nil {
			return 0, 0, false, err
		}
		line, readErr := reader.ReadString('\n')
		if readErr != nil && readErr != io.EOF {
			return 0, 0, false, readErr
		}
		next := offset + int64(len(line))
		kind, detail := progressRecord(line)
		switch kind {
		case "turn/started":
			if start == -1 && detail == id {
				start = offset
			}
		case "turn/start":
			if start == -1 && strings.HasSuffix(detail, " → "+id) {
				start = offset
			}
		case "turn/completed":
			completed, _, _ := strings.Cut(detail, " ")
			if completed == id {
				if start == -1 {
					start = previousEnd
				}
				return start, next, true, nil
			}
			previousEnd = next
		}
		if readErr == io.EOF {
			break
		}
		offset = next
	}
	if start == -1 {
		start = previousEnd
	}
	return start, size, false, nil
}

type resultOutput struct {
	Status        *string            `json:"status"`
	ExitCode      *int64             `json:"exit_code"`
	LogPath       *string            `json:"log_path"`
	Job           string             `json:"job"`
	Engine        string             `json:"engine"`
	ReportPath    string             `json:"report_path"`
	PatchManifest string             `json:"patch_manifest"`
	Artifacts     []SnapshotArtifact `json:"artifacts"`
	Turn          int64              `json:"turn"`
}

func displayResultDetails(out io.Writer, dir, id string, logOnly bool) error {
	manifest := filepath.Join(dir, "result.json")
	var m turnResult
	if err := readJSON(manifest, &m); err != nil {
		return err
	}
	if m.ResultDetails == nil {
		return errors.New("result archive lacks details")
	}
	var logPath *string
	switch m.Log {
	case "":
	case "output.log", "progress.log":
		path := filepath.Join(dir, m.Log)
		logPath = &path
	default:
		return errors.New("invalid result log path")
	}
	if logOnly {
		if logPath == nil {
			diagnosticf("cx: result log unavailable\n")
			return ctlExit(9)
		}
		log, err := openJobFile(*logPath, jobFileRead)
		if err != nil {
			return err
		}
		defer closeBestEffort(log)
		_, err = io.Copy(out, log)
		return err
	}
	artifacts := make([]SnapshotArtifact, 0, len(m.Artifacts))
	for _, row := range m.Artifacts {
		row.Path = filepath.Join(dir, row.Path)
		artifacts = append(artifacts, row)
	}
	return json.NewEncoder(out).Encode(resultOutput{
		Job: id, Engine: m.Engine, Status: m.Status, ExitCode: m.ExitCode, Turn: m.Turn,
		ReportPath: filepath.Join(dir, "last.md"), LogPath: logPath, PatchManifest: manifest, Artifacts: artifacts,
	})
}
