package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/djylb/timeshim"
)

const transportLogLimit = 256 << 10
const transportRecordLimit = 4096

type transportEvent struct {
	Time           time.Time `json:"time"`
	Host           string    `json:"host"`
	Event          string    `json:"event"`
	Reason         string    `json:"reason"`
	DisconnectedMS *int64    `json:"disconnected_ms,omitempty"`
}

func transportLogPath(host string) (string, error) {
	home, err := controllerCacheRoot()
	if err != nil {
		return "", err
	}
	path, err := jobPath(filepath.Join(home, "transport"), host)
	if err != nil {
		return "", err
	}
	return path + ".log", nil
}

func transportEvents(data []byte) []transportEvent {
	data = data[:bytes.LastIndexByte(data, '\n')+1]
	var events []transportEvent
	for line := range bytes.SplitSeq(data, []byte{'\n'}) {
		var event transportEvent
		if len(line) != 0 && json.Unmarshal(line, &event) == nil && !event.Time.IsZero() &&
			(event.Event == "disconnect" || event.Event == "reconnect") {
			events = append(events, event)
		}
	}
	return events
}

func transportTail(path string) (transportEvent, int64, bool, error) {
	in, err := openJobFile(path, jobFileRead)
	if os.IsNotExist(err) {
		return transportEvent{}, 0, false, nil
	}
	if err != nil {
		return transportEvent{}, 0, false, err
	}
	defer closeBestEffort(in)
	info, err := in.Stat()
	if err != nil {
		return transportEvent{}, 0, false, err
	}
	offset := max(int64(0), info.Size()-2*transportRecordLimit)
	b, err := io.ReadAll(io.NewSectionReader(in, offset, info.Size()-offset))
	if err != nil {
		return transportEvent{}, 0, false, err
	}
	torn := len(b) != 0 && b[len(b)-1] != '\n'
	events := transportEvents(b)
	if len(events) != 0 {
		return events[len(events)-1], info.Size(), torn, nil
	}
	return transportEvent{}, info.Size(), torn, nil
}

func recordTransportSample(path, host, reason string, at time.Time, limit int64) error {
	if reason == "" {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			if _, prevErr := os.Stat(path + ".1"); os.IsNotExist(prevErr) {
				return nil
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lock, err := lockUntilFor(ctx, path+".lock", time.Second)
	if err != nil {
		return err
	}
	defer closeBestEffort(lock)
	latest, size, torn, err := transportTail(path)
	if err != nil {
		return err
	}
	if latest.Time.IsZero() {
		latest, _, _, err = transportTail(path + ".1")
		if err != nil {
			return err
		}
	}
	at = at.UTC()
	if at.Before(latest.Time) {
		return nil
	}
	event := transportEvent{Time: at, Host: host}
	if reason == "" {
		if latest.Event != "disconnect" {
			return nil
		}
		ms := at.Sub(latest.Time).Milliseconds()
		event.Event, event.Reason, event.DisconnectedMS = "reconnect", latest.Reason, &ms
	} else {
		if latest.Event == "disconnect" {
			return nil
		}
		event.Event, event.Reason = "disconnect", reason
	}
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if int64(len(b)) > min(limit, transportRecordLimit) {
		return errors.New("transport event exceeds the record limit")
	}
	appendSize := int64(len(b))
	if torn {
		appendSize++
	}
	if size+appendSize > limit {
		if err = os.Rename(path, path+".1"); err != nil {
			return err
		}
	} else if torn {
		b = append([]byte{'\n'}, b...)
	}
	out, err := openJobFile(path, jobFileAppendCreate)
	if err != nil {
		return err
	}
	return errors.Join(writeFrame(out.Write, b), out.Close())
}

func transportFailure(remote, ready bool, err error) (string, bool) {
	switch {
	case err == nil:
		return "", true
	case errors.Is(err, errSSHMasterUnsupported):
		return "", false
	case errors.Is(err, errWireSilence):
		if ready {
			return "wire silence", true
		}
		return "dial timeout", true
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "", false
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		if remote && exit.ExitCode() == 255 {
			if ready {
				return "connection lost", true
			}
			return "dial failed", true
		}
		return "", exit.ExitCode() >= 0
	}
	var start *exec.Error
	if errors.As(err, &start) {
		return "transport start failed", true
	}
	return "", false
}

func (p *transportProcess) noteTransport(ready bool, err error) {
	if p.host == "" {
		return
	}
	reason, observed := transportFailure(p.remote, ready, err)
	if !observed || reason == "" && !ready {
		return
	}
	path, pathErr := transportLogPath(p.host)
	if pathErr == nil {
		at := timeshim.Or(contextClock(p.ctx)).Now()
		pathErr = recordTransportSample(path, p.host, reason, at, transportLogLimit)
	}
	if pathErr != nil {
		diagnosticf("cx: %s transport log: %v\n", p.host, pathErr)
	}
}

func recentTransportEvents(host string, n int) ([]transportEvent, error) {
	path, err := transportLogPath(host)
	if err != nil {
		return nil, err
	}
	events := make([]transportEvent, 0, n) // JSON keeps an empty list, not null
	if _, err = os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
		return events, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lock, err := lockUntilFor(ctx, path+".lock", time.Second)
	if err != nil {
		return nil, err
	}
	defer closeBestEffort(lock)
	var files [][]byte
	for _, name := range []string{path + ".1", path} {
		f, err := openJobFile(name, jobFileRead)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		b, readErr := io.ReadAll(io.LimitReader(f, transportLogLimit))
		if err = errors.Join(readErr, f.Close()); err != nil {
			return nil, err
		}
		files = append(files, b)
	}
	closeBestEffort(lock)
	for _, b := range files {
		events = append(events, transportEvents(b)...)
	}
	return events[max(0, len(events)-n):], nil
}

func addRecentTransport(host string, report fields) {
	events, err := recentTransportEvents(host, 10)
	if err != nil {
		report["transport_log_error"] = err.Error()
		return
	}
	report["recent_transport"] = events
	if len(events) != 0 && events[len(events)-1].Event == "disconnect" {
		report["current_outage_ms"] = max(0, time.Since(events[len(events)-1].Time).Milliseconds())
	}
}
