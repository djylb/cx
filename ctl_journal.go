package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type pendingDecision struct {
	Channel string      `json:"channel"`
	Token   string      `json:"token"`
	Message *channelRow `json:"message"`
	Offset  int64       `json:"offset"`
}

func journalDecision(ctx context.Context, h endpoint, q channelQuery) ([]channelRow, error) {
	dir, e := outputProject(q.Project)
	if e != nil {
		return nil, e
	}
	l, e := lockUntil(ctx, filepath.Join(dir, ".decisions.lock"))
	if e != nil {
		return nil, e
	}
	defer l.Close()
	pending := filepath.Join(dir, ".decision-pending.json")
	log := filepath.Join(dir, "decisions.jsonl")
	finish := func(d pendingDecision) ([]channelRow, error) {
		replay := q
		replay.Channel, replay.Token, replay.Message = d.Channel, d.Token, d.Message
		rows, e := channelCall(ctx, h, replay)
		if e != nil {
			return nil, e
		}
		if len(rows) != 1 {
			return nil, errors.New("missing decision receipt")
		}
		record := struct {
			channelRow
			Channel string `json:"channel"`
		}{rows[0], d.Channel}
		b, e := json.Marshal(record)
		if e != nil {
			return nil, e
		}
		b = append(b, '\n')
		f, e := os.OpenFile(log, os.O_CREATE|os.O_RDWR, 0600)
		if e != nil {
			return nil, e
		}
		defer f.Close()
		st, e := f.Stat()
		if e != nil {
			return nil, e
		}
		n := st.Size() - d.Offset
		if n < 0 || n > int64(len(b)) {
			return nil, errors.New("decision log changed during recovery")
		}
		prev := make([]byte, n)
		if _, e = f.ReadAt(prev, d.Offset); e != nil && len(prev) > 0 {
			return nil, e
		}
		if !bytes.HasPrefix(b, prev) {
			return nil, errors.New("decision log disagrees with pending record")
		}
		if _, e = f.WriteAt(b, d.Offset); e != nil {
			return nil, e
		}
		if e = f.Sync(); e != nil {
			return nil, e
		}
		return rows, os.Remove(pending)
	}
	var old pendingDecision
	if e = readJSON(pending, &old); e == nil {
		rows, e := finish(old)
		if e != nil {
			return nil, e
		}
		prior := *old.Message
		prior.Time = q.Message.Time
		if old.Channel == q.Channel && prior == *q.Message {
			return rows, nil
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	d := pendingDecision{q.Channel, q.Token, q.Message, 0}
	if st, e := os.Stat(log); e == nil {
		d.Offset = st.Size()
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	if e = writeJSON(pending, d); e != nil {
		return nil, e
	}
	return finish(d)
}
