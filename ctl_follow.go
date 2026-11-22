package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type tailPosition struct {
	Mode          string `json:"mode"`
	Next          int64  `json:"next"`
	Raw           bool   `json:"raw"`
	Since         int64  `json:"since,omitempty"`
	Finished      int64  `json:"finished,omitempty"`
	LastCompleted string `json:"last_completed,omitempty"`
}
type tailFrame struct {
	Terminal *status      `json:"terminal,omitempty"`
	Data     []byte       `json:"data,omitempty"`
	Position tailPosition `json:"position"`
}

// Frame complete records with their successor cursor. A partial SSH frame
// never advances the consumer, including for binary output without newlines.
type frameWriter struct {
	enc     *json.Encoder
	pos     tailPosition
	pending bytes.Buffer
}

func (w *frameWriter) Write(p []byte) (int, error) {
	if w.pos.Mode == "offset" {
		w.pos.Next += int64(len(p))
		return len(p), w.enc.Encode(tailFrame{Data: p, Position: w.pos})
	}
	w.pending.Write(p)
	for {
		line, e := w.pending.ReadBytes('\n')
		if e == io.EOF {
			w.pending.Write(line)
			break
		}
		if e != nil {
			return 0, e
		}
		cols := bytes.SplitN(line, []byte("\t"), 4)
		if len(cols) != 4 {
			continue
		}
		n, e := strconv.ParseInt(string(cols[0]), 10, 64)
		if e != nil {
			return 0, e
		}
		w.pos.Next = n
		if w.pos.Raw || keyEvent.Match(cols[2]) {
			if e = w.enc.Encode(tailFrame{Data: line, Position: w.pos}); e != nil {
				return 0, e
			}
		}
	}
	return len(p), nil
}
func streamLog(ctx context.Context, dir string, pos tailPosition) error {
	if pos.Mode == "messages" {
		return streamMessages(ctx, dir, pos)
	}
	var s status
	if e := readJSON(filepath.Join(dir, "status.json"), &s); e != nil {
		return e
	}
	if pos.Mode == "since" {
		pos.Mode = "seq"
		if pos.Raw && s.Engine == "cmd" {
			pos.Mode = "offset"
		}
	}
	w := frameWriter{enc: json.NewEncoder(os.Stdout), pos: pos}
	for ctx.Err() == nil {
		var e error
		v := strconv.FormatInt(w.pos.Next, 10)
		if w.pos.Mode == "seq" {
			e = writeLogSince(&w, dir, v)
		} else {
			e = writeLogFrom(&w, dir, []string{v})
		}
		frame := tailFrame{Position: w.pos}
		if errors.Is(e, errDrained) {
			if e = readJSON(filepath.Join(dir, "status.json"), &s); e != nil {
				return e
			}
			frame.Terminal = &s
			return w.enc.Encode(frame)
		}
		if e != nil {
			return e
		}
		if e = w.enc.Encode(frame); e != nil {
			return e
		}
		_ = pause(ctx, time.Second)
	}
	return errObserveTimeout
}
func follow(ctx context.Context, h endpoint, name, id string, o ctlOptions) error {
	limit, e := observation(o.timeout)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, limit+h.connectionBudget(time.Second))
	defer cancel()
	pos := tailPosition{Mode: "seq", Raw: o.raw}
	if o.raw {
		pos.Mode = "offset"
	}
	if o.since != "" {
		pos.Mode = "since"
		pos.Next, e = strconv.ParseInt(o.since, 10, 64)
		if e != nil || pos.Next < 0 {
			return errors.New("invalid since")
		}
	}
	if o.messages {
		pos = tailPosition{Mode: "messages", Since: pos.Next}
	}
	path := ""
	if o.cursor != "" {
		p, j, ok := strings.Cut(id, "/")
		if !ok {
			p, j = "flat", p
		}
		mode := strconv.FormatBool(o.raw)
		if o.messages {
			mode = "messages"
		}
		path, e = cursorFile("tail", name, p, j, mode, o.cursor)
		if e != nil {
			return e
		}
		lock, e := lockUntil(ctx, path+".lock")
		if e != nil {
			return e
		}
		defer closeBestEffort(lock)
		if o.since == "" && !o.fromStart {
			if e = readJSON(path, &pos); e != nil && !os.IsNotExist(e) {
				return e
			}
		}
	}
	for i := 0; i < 4; i++ {
		attempt, stop := context.WithCancel(ctx)
		b, _ := json.Marshal(pos)
		cmd, e := h.command(attempt, []string{"follow", id, string(b), limit.String()})
		if e != nil {
			stop()
			return e
		}
		rd, wr := io.Pipe()
		var diag bytes.Buffer
		done := make(chan error, 1)
		go func() {
			err := runActive(cmd, stop, h.connectionBudget(10*time.Second), 10*time.Second, wr, &diag)
			_ = wr.CloseWithError(err)
			done <- err
		}()
		var input io.Reader = rd
		var compressed *gzip.Reader
		if o.messages {
			compressed, e = gzip.NewReader(rd)
			if e == nil {
				input = compressed
			}
		}
		dec := json.NewDecoder(input)
		var terminal *status
		for e == nil {
			var frame tailFrame
			e = dec.Decode(&frame)
			if e != nil {
				break
			}
			if _, e = os.Stdout.Write(frame.Data); e != nil {
				break
			}
			if frame.Position != pos && path != "" {
				if e = writeJSON(path, frame.Position); e != nil {
					break
				}
			}
			pos = frame.Position
			terminal = frame.Terminal
		}
		if compressed != nil {
			closeBestEffort(compressed)
		}
		_ = rd.Close()
		stop()
		processErr := <-done
		if terminal != nil && errors.Is(e, io.EOF) {
			if o.wait {
				return resultCode(*terminal, 0)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctlExit(124)
		}
		if processErr == nil {
			return fmt.Errorf("incomplete tail: %w", e)
		}
		if !unsupportedMaster.Load() && unsupportedDiagnostic(runtime.GOOS, diag.String()) {
			unsupportedMaster.Store(true)
			i--
			continue
		}
		var ex *exec.ExitError
		if errors.As(processErr, &ex) && ex.ExitCode() != 255 {
			fmt.Fprint(os.Stderr, cleanDiag(diag.Bytes()))
			return ctlExit(ex.ExitCode())
		}
		if i < 3 {
			if e = pause(ctx, time.Second<<i); e != nil {
				return ctlExit(124)
			}
		}
	}
	return ctlExit(9)
}

// streamMessages follows the same framed, resumable observation protocol as
// progress tail. Flush each frame so an idle job does not delay delivery.
func streamMessages(ctx context.Context, dir string, pos tailPosition) (err error) {
	compressed := gzip.NewWriter(os.Stdout)
	defer func() { err = errors.Join(err, compressed.Close()) }()
	enc := json.NewEncoder(compressed)
	emit := func(frame tailFrame) error {
		if e := enc.Encode(frame); e != nil {
			return e
		}
		return compressed.Flush()
	}
	for ctx.Err() == nil {
		var s status
		if e := readJSON(filepath.Join(dir, "status.json"), &s); e != nil {
			return e
		}
		drained, e := readMessageEvents(ctx, dir, &pos, emit)
		if e != nil {
			return e
		}
		frame := tailFrame{Position: pos}
		if drained && isTerminal(s.State) {
			frame.Terminal = &s
			return emit(frame)
		}
		if e = emit(frame); e != nil {
			return e
		}
		_ = pause(ctx, time.Second)
	}
	return errObserveTimeout
}

func readMessageEvents(ctx context.Context, dir string, pos *tailPosition, emit func(tailFrame) error) (bool, error) {
	f, err := os.Open(filepath.Join(dir, "events.jsonl"))
	if os.IsNotExist(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer closeBestEffort(f)
	if _, err = f.Seek(pos.Next, io.SeekStart); err != nil {
		return false, err
	}
	rd := bufio.NewReader(f)
	for ctx.Err() == nil {
		line, e := rd.ReadBytes('\n')
		if e == io.EOF {
			return len(line) == 0, nil
		}
		if e != nil {
			return false, e
		}
		var record struct {
			Seq        int64
			Method     string
			Time       string
			TurnNumber int64 `json:"turn_number"`
			Params     json.RawMessage
		}
		if e = json.Unmarshal(line, &record); e != nil {
			return false, e
		}
		start := pos.Next + 1
		pos.Next += int64(len(line))
		if record.Seq == 0 {
			record.Seq = start
		}
		switch record.Method {
		case "turn/completed":
			var ev event
			if e = json.Unmarshal(record.Params, &ev); e != nil {
				return false, e
			}
			if ev.Turn.ID != "" && ev.Turn.ID != pos.LastCompleted {
				pos.Finished++
				pos.LastCompleted = ev.Turn.ID
			}
		case "item/completed":
			if record.Seq <= pos.Since {
				continue
			}
			var ev event
			if e = json.Unmarshal(record.Params, &ev); e != nil {
				return false, e
			}
			if ev.Item.Type != "agentMessage" {
				continue
			}
			if record.TurnNumber == 0 {
				record.TurnNumber = pos.Finished + 1
				if ev.TurnID != "" && ev.TurnID == pos.LastCompleted {
					record.TurnNumber = pos.Finished
				}
			}
			if record.Time == "" {
				record.Time = "unknown"
			}
			data := fmt.Appendf(nil, "--- turn %d, %s\n%s", record.TurnNumber, record.Time, ev.Item.Text)
			if !strings.HasSuffix(ev.Item.Text, "\n") {
				data = append(data, '\n')
			}
			if e = emit(tailFrame{Data: data, Position: *pos}); e != nil {
				return false, e
			}
		}
	}
	return false, ctx.Err()
}
