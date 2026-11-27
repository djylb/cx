package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/djylb/timeshim"
)

const waitPollInterval = 30 * time.Second

type waitDelivery struct {
	row    any
	before []byte
	cursor []byte
	code   int
}

type waitSource struct {
	poll      func(context.Context) (waitDelivery, error)
	file      string
	label     string
	available atomic.Bool
	jobHost   bool
}

// Unavailable observers stay alive and retry. Cursor failures are local
// errors, so retrying a node call cannot resolve them.
type waitUnavailable struct{ error }

func waitRPC(ctx context.Context, h endpoint, limit time.Duration, op string, req any) (reply, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return reply{}, err
	}
	const transportGrace = 15 * time.Second
	attempt, cancel := timeshim.WithTimeout(ctx, timeshim.Or(contextClock(ctx)), limit+transportGrace)
	defer cancel()
	r, err := h.invokeWithDiagnostics(attempt, limit+2*time.Second, false, false, op, string(b))
	if err != nil {
		return reply{}, waitUnavailable{err}
	}
	if r.code != 0 && r.code != 124 {
		return reply{}, waitUnavailable{ctlExit(r.code)}
	}
	return r, nil
}

func initializeJobWaitCursor(ctx context.Context, h endpoint, file string, req waitPollRequest) error {
	req.Cursor, req.Projects, req.Jobs = nil, nil, nil
	r, err := waitRPC(ctx, h, req.Limit, "wait-poll", req)
	if err != nil {
		return err
	}
	if r.code == 124 {
		return waitUnavailable{errObserveTimeout}
	}
	var result waitPollResponse
	if err = json.Unmarshal(r.data, &result); err != nil {
		return waitUnavailable{err}
	}
	if result.Row != nil {
		return waitUnavailable{errors.New("unexpected event during cursor initialization")}
	}
	if len(result.Cursor) == 0 {
		return waitUnavailable{errors.New("missing job cursor")}
	}
	if _, err = decodeWaitBaseline(result.Cursor); err != nil {
		return waitUnavailable{err}
	}
	// Even an empty cursor records that this host reached its initialization
	// boundary. Only subsequent jobs and events are eligible for delivery.
	_, _, err = updateWaitCursor(ctx, file, nil, result.Cursor, nil)
	return err
}

func initializeWaitHosts(ctx context.Context, hs endpoints, dir string, req waitPollRequest) error {
	names := slices.Sorted(maps.Keys(hs))
	results := make([]error, len(names))
	var workers sync.WaitGroup
	for i, name := range names {
		workers.Go(func() {
			file := filepath.Join(dir, name+".json")
			saved, err := readWaitCursorFile(file)
			if err == nil && saved == nil {
				err = initializeJobWaitCursor(ctx, hs[name], file, req)
			}
			results[i] = err
		})
	}
	workers.Wait()
	for i, err := range results {
		if err == nil {
			continue
		}
		if _, ok := errors.AsType[waitUnavailable](err); !ok {
			return err
		}
		diagnosticf("cx wait --any: %s cursor initialization unavailable: %s\n", names[i], brief(err.Error()))
	}
	return nil
}

func jobWaitPoll(h endpoint, name, file string, req waitPollRequest) func(context.Context) (waitDelivery, error) {
	return func(ctx context.Context) (waitDelivery, error) {
		before, err := readWaitCursorFile(file)
		if err != nil {
			return waitDelivery{}, err
		}
		if before == nil {
			return waitDelivery{}, initializeJobWaitCursor(ctx, h, file, req)
		}
		req.Cursor, err = decodeWaitBaseline(before)
		if err != nil {
			return waitDelivery{}, fmt.Errorf("%s: %w", name, err)
		}
		r, err := waitRPC(ctx, h, req.Limit, "wait-poll", req)
		if err != nil || r.code == 124 {
			return waitDelivery{}, err
		}
		var result waitPollResponse
		if err = json.Unmarshal(r.data, &result); err != nil {
			return waitDelivery{}, waitUnavailable{err}
		}
		if len(result.Cursor) == 0 {
			return waitDelivery{}, waitUnavailable{errors.New("missing job cursor")}
		}
		next, err := decodeWaitBaseline(result.Cursor)
		if err != nil {
			return waitDelivery{}, waitUnavailable{err}
		}
		if result.Row == nil {
			return waitDelivery{}, waitUnavailable{errors.New("missing job event")}
		}
		id := waitJobID(result.Row.Project, result.Row.Job)
		position, ok := next[id]
		if !ok || result.Row.EventID == "" {
			return waitDelivery{}, waitUnavailable{errors.New("missing job event position or identity")}
		}
		last := req.Cursor[id]
		position.EventID = result.Row.EventID
		next[id] = position
		cursor, err := json.Marshal(next)
		if err != nil {
			return waitDelivery{}, err
		}
		if last.DirKey == position.DirKey && last.EventID == position.EventID {
			_, _, err = updateWaitCursor(ctx, file, before, cursor, nil)
			return waitDelivery{}, err
		}
		result.Row.Host = name
		result.Row.setNext(name)
		return waitDelivery{row: result.Row, before: before, cursor: cursor, code: result.Row.Code}, nil
	}
}

func chatWaitPoll(h endpoint, file, channel string, q channelQuery) func(context.Context) (waitDelivery, error) {
	return func(ctx context.Context) (waitDelivery, error) {
		before, err := readWaitCursorFile(file)
		if err != nil {
			return waitDelivery{}, err
		}
		q.Since = 0
		if before != nil {
			if err = json.Unmarshal(before, &q.Since); err != nil {
				return waitDelivery{}, err
			}
		}
		r, err := waitRPC(ctx, h, q.Wait, "channel", q)
		if err != nil || r.code == 124 {
			return waitDelivery{}, err
		}
		var rows []channelRow
		if err = json.Unmarshal(r.data, &rows); err != nil {
			return waitDelivery{}, waitUnavailable{err}
		}
		if len(rows) == 0 {
			return waitDelivery{}, waitUnavailable{errors.New("missing channel event")}
		}
		row := struct {
			EventID string `json:"event_id"`
			Event   string `json:"event"`
			Channel string `json:"channel"`
			Project string `json:"project"`
			channelRow
			Code int `json:"code"`
		}{channelRow: rows[0], Channel: channel, Project: q.Project, Event: "chat",
			EventID: waitEventID(q.Project, "chat", fmt.Sprintf("%s/%d", q.Channel, rows[0].Seq))}
		return waitDelivery{row: row, before: before, cursor: fmt.Append(nil, rows[0].Seq)}, nil
	}
}

func (s *waitSource) watch(ctx context.Context, warn func(string, ...any)) (waitDelivery, error) {
	clock := timeshim.Or(contextClock(ctx))
	var delay time.Duration
	for ctx.Err() == nil {
		event, err := s.poll(ctx)
		if ctx.Err() != nil {
			break
		}
		if err == nil {
			s.available.Store(true)
			delay = 0
			if event.row != nil {
				return event, nil
			}
			continue
		}
		if _, ok := errors.AsType[waitUnavailable](err); !ok {
			return waitDelivery{}, err
		}
		s.available.Store(false)
		if delay == 0 {
			warn("cx wait --any: %s unavailable, retrying: %s\n", s.label, brief(err.Error()))
		}
		delay = min(max(time.Second, 2*delay), waitPollInterval)
		if timeshim.Sleep(ctx, clock, delay) != nil {
			break
		}
	}
	return waitDelivery{}, ctx.Err()
}

func waitForAny(ctx context.Context, sources []*waitSource, out io.Writer, warn func(string, ...any)) error {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() {
		cancel()
		workers.Wait()
	}()
	type watched struct {
		err      error
		source   *waitSource
		delivery waitDelivery
	}
	results := make(chan watched, len(sources))
	start := func(s *waitSource) {
		workers.Go(func() {
			delivery, err := s.watch(ctx, warn)
			results <- watched{err: err, source: s, delivery: delivery}
		})
	}
	for _, s := range sources {
		start(s)
	}
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case result := <-results:
			if ctx.Err() != nil {
				break
			}
			if result.err != nil {
				return result.err
			}
			d := result.delivery
			_, advanced, err := updateWaitCursor(ctx, result.source.file, d.before, d.cursor, func() error {
				return json.NewEncoder(out).Encode(d.row)
			})
			if err != nil {
				if cause := ctx.Err(); cause != nil && errors.Is(err, cause) {
					break
				}
				return err
			}
			if !advanced {
				start(result.source)
				continue
			}
			if d.code != 0 {
				return ctlExit(d.code)
			}
			return nil
		}
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ctx.Err()
	}
	var offline []string
	for _, s := range sources {
		if s.jobHost {
			if s.available.Load() {
				return ctlExit(124)
			}
			offline = append(offline, s.label)
		}
	}
	if len(offline) > 0 {
		warn("cx wait --any: timed out with all selected hosts unavailable (%s)\n", strings.Join(offline, ", "))
	}
	return ctlExit(124)
}

func ctlWaitAny(ctx context.Context, hs endpoints, o ctlOptions) error {
	if len(o.pos) != 0 {
		return errors.New("wait --any takes no job")
	}
	for host := range o.waitJobsByHost {
		if _, err := hs.get(host); err != nil {
			return ctlUsage("wait", err)
		}
	}
	limit, err := observation(o.timeout)
	if err != nil {
		return err
	}
	ctx, cancel := timeshim.WithTimeout(ctx, timeshim.Or(contextClock(ctx)), limit)
	defer cancel()
	if o.cursor == "" {
		o.cursor = "default"
	}
	parts := []string{"wait", o.cursor}
	if o.session != "" {
		parts = append(parts, "sessions", snapSum([]byte(o.session)))
	}
	path, err := cursorFile(append(parts, "cursor")...)
	if err != nil {
		return err
	}
	release, err := registerWaitObserver(ctx, path+".lock", fmt.Sprintf("cursor %q for session %q", o.cursor, o.session),
		"use --cursor NAME for an independent cursor")
	if err != nil {
		return err
	}
	defer release()
	dir := filepath.Dir(path)
	names := o.hosts
	if len(names) == 0 {
		names = hs.names()
	}
	var sources []*waitSource
	seen := make(map[string]bool)
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		h, hostErr := hs.get(name)
		if hostErr != nil {
			return hostErr
		}
		jobs := o.waitJobsByHost[name]
		if len(o.waitJobsByHost) > 0 && len(jobs) == 0 {
			continue
		}
		file := filepath.Join(dir, name+".json")
		req := waitPollRequest{Projects: o.projects, Jobs: jobs, Session: o.session, Limit: waitPollInterval, FromStart: o.fromStart}
		sources = append(sources, &waitSource{poll: jobWaitPoll(h, name, file, req), file: file, label: name, jobHost: true})
	}
	if len(o.channels) > 0 {
		h, root, centerErr := channelCenter(hs)
		if centerErr != nil {
			return centerErr
		}
		projects := o.projects
		if len(projects) == 0 {
			p, projectErr := defaultProject(ctx)
			if projectErr != nil {
				return projectErr
			}
			projects = []string{p}
		}
		for _, p := range projects {
			for _, channel := range o.channels {
				key := strings.TrimPrefix(channel, "#")
				if _, err = jobDir("", p+"/"+key); err != nil {
					return err
				}
				file := filepath.Join(dir, "chat", p, key+".json")
				if seen[file] {
					continue
				}
				seen[file] = true
				if err = os.MkdirAll(filepath.Dir(file), 0700); err != nil {
					return err
				}
				q := channelQuery{Project: p, Channel: key, Root: root, Wait: waitPollInterval}
				sources = append(sources, &waitSource{poll: chatWaitPoll(h, file, channel, q), file: file, label: p + "/#" + key})
			}
		}
	}
	req := waitPollRequest{Session: o.session, Limit: waitPollInterval, FromStart: o.fromStart}
	if err = initializeWaitHosts(ctx, hs, dir, req); err != nil && ctx.Err() == nil {
		return err
	}
	return waitForAny(ctx, sources, os.Stdout, diagnosticf)
}
