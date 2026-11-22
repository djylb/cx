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
	Message *channelRow `json:"message"`
	Channel string      `json:"channel"`
	Token   string      `json:"token"`
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
	defer closeBestEffort(l)
	pending := filepath.Join(dir, ".decision-pending.json")
	log := filepath.Join(dir, "decisions.jsonl")
	finish := func(d pendingDecision) ([]channelRow, error) {
		replay := q
		replay.Channel, replay.Token, replay.Message = d.Channel, d.Token, d.Message
		rows, journalErr := channelCall(ctx, h, replay)
		if journalErr != nil {
			return nil, journalErr
		}
		if len(rows) != 1 {
			return nil, errors.New("missing decision receipt")
		}
		record := struct {
			Channel string `json:"channel"`
			channelRow
		}{channelRow: rows[0], Channel: d.Channel}
		b, journalErr := json.Marshal(record)
		if journalErr != nil {
			return nil, journalErr
		}
		b = append(b, '\n')
		f, journalErr := os.OpenFile(log, os.O_CREATE|os.O_RDWR, 0600)
		if journalErr != nil {
			return nil, journalErr
		}
		defer closeBestEffort(f)
		st, journalErr := f.Stat()
		if journalErr != nil {
			return nil, journalErr
		}
		n := st.Size() - d.Offset
		if n < 0 || n > int64(len(b)) {
			return nil, errors.New("decision log changed during recovery")
		}
		prev := make([]byte, n)
		if _, journalErr = f.ReadAt(prev, d.Offset); journalErr != nil && len(prev) > 0 {
			return nil, journalErr
		}
		if !bytes.HasPrefix(b, prev) {
			return nil, errors.New("decision log disagrees with pending record")
		}
		if _, journalErr = f.WriteAt(b, d.Offset); journalErr != nil {
			return nil, journalErr
		}
		if journalErr = f.Sync(); journalErr != nil {
			return nil, journalErr
		}
		return rows, os.Remove(pending)
	}
	var old pendingDecision
	if e = readJSON(pending, &old); e == nil {
		rows, finishErr := finish(old)
		if finishErr != nil {
			return nil, finishErr
		}
		prior := *old.Message
		prior.Time = q.Message.Time
		if old.Channel == q.Channel && prior == *q.Message {
			return rows, nil
		}
	} else if !os.IsNotExist(e) {
		return nil, e
	}
	d := pendingDecision{Channel: q.Channel, Token: q.Token, Message: q.Message}
	if st, statErr := os.Stat(log); statErr == nil {
		d.Offset = st.Size()
	} else if !os.IsNotExist(statErr) {
		return nil, statErr
	}
	if e = writeJSON(pending, d); e != nil {
		return nil, e
	}
	return finish(d)
}
