package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

func jobWaitPoll(h endpoint, name, file string, req waitPollRequest) func(context.Context) (waitDelivery, error) {
	return func(ctx context.Context) (waitDelivery, error) {
		before, err := readWaitCursorFile(file)
		if err != nil {
			return waitDelivery{}, err
		}
		req.Cursor = nil
		if before != nil {
			req.Cursor, err = decodeWaitBaseline(before)
			if err != nil {
				return waitDelivery{}, fmt.Errorf("%s: %w", name, err)
			}
		}
		r, err := waitRPC(ctx, h, req.Limit, "wait-poll", req)
		if err != nil {
			return waitDelivery{}, err
		}
		if r.code == 124 {
			if before == nil {
				return waitDelivery{}, waitUnavailable{errObserveTimeout}
			}
			return waitDelivery{}, nil
		}
		var result waitPollResponse
		if err = json.Unmarshal(r.data, &result); err != nil {
			return waitDelivery{}, waitUnavailable{err}
		}
		if len(result.Cursor) == 0 {
			return waitDelivery{}, waitUnavailable{errors.New("missing job cursor")}
		}
		if _, err = decodeWaitBaseline(result.Cursor); err != nil {
			return waitDelivery{}, waitUnavailable{err}
		}
		if before == nil {
			if result.Row != nil {
				return waitDelivery{}, waitUnavailable{errors.New("unexpected event during cursor initialization")}
			}
			_, err = updateWaitCursor(ctx, file, nil, result.Cursor)
			return waitDelivery{}, err
		}
		if result.Row == nil {
			return waitDelivery{}, waitUnavailable{errors.New("missing job event")}
		}
		result.Row.Host = name
		result.Row.setNext(name)
		return waitDelivery{row: result.Row, before: before, cursor: result.Cursor, code: result.Row.Code}, nil
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
		var unavailable waitUnavailable
		if !errors.As(err, &unavailable) {
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
	for _, s := range sources {
		workers.Go(func() {
			delivery, err := s.watch(ctx, warn)
			results <- watched{err: err, source: s, delivery: delivery}
		})
	}
	select {
	case result := <-results:
		if result.err == nil && ctx.Err() == nil {
			d := result.delivery
			if err := json.NewEncoder(out).Encode(d.row); err != nil {
				return err
			}
			if _, err := updateWaitCursor(ctx, result.source.file, d.before, d.cursor); err != nil {
				return err
			}
			if d.code != 0 {
				return ctlExit(d.code)
			}
			return nil
		}
		if ctx.Err() == nil {
			return result.err
		}
	case <-ctx.Done():
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
	lock, err := registerWaitObserver(ctx, path+".lock", fmt.Sprintf("cursor %q", o.cursor))
	if err != nil {
		return err
	}
	defer closeBestEffort(lock)
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
		file := filepath.Join(dir, name+".json")
		req := waitPollRequest{Projects: o.projects, Session: o.session, Limit: waitPollInterval, FromStart: o.fromStart}
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
	return waitForAny(ctx, sources, os.Stdout, diagnosticf)
}
