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
	"runtime"
	"strconv"
	"strings"
	"time"
)

type tailPosition struct {
	Mode          string `json:"mode"`
	DirectoryID   string `json:"directory_id,omitempty"`
	StateCursor   string `json:"state_cursor,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	LastCompleted string `json:"last_completed,omitempty"`
	Next          int64  `json:"next"`
	Since         int64  `json:"since,omitempty"`
	Finished      int64  `json:"finished,omitempty"`
	Raw           bool   `json:"raw"`
}
type tailFrame struct {
	Terminal *status      `json:"terminal,omitempty"`
	Data     []byte       `json:"data,omitempty"`
	Position tailPosition `json:"position"`
}

func (pos *tailPosition) openObservation(dir string) (*jobStateObserver, error) {
	if pos.Next < 0 || pos.Since < 0 {
		return nil, errors.New("invalid follow position")
	}
	var cursor stateCursor
	if pos.StateCursor != "" {
		var err error
		cursor, err = parseCursor(pos.StateCursor)
		if err != nil {
			return nil, err
		}
	}
	observer, err := openJobObservation(dir, cursor, pos.RunID)
	if err != nil {
		return nil, err
	}
	key := observer.identity.key
	if pos.RunID != "" && (pos.RunID != observer.s.RunID || pos.DirectoryID != key) {
		// Replace the old anchor on the already pinned observer. The status
		// and log positions must all start on the same directory identity.
		tail, err := openStateTail(dir, observer.cursor)
		if err != nil {
			closeBestEffort(observer)
			return nil, err
		}
		closeBestEffort(observer.tail)
		observer.tail = tail
		mode := pos.Mode
		if mode != "messages" {
			mode = "since"
		}
		*pos = tailPosition{Mode: mode, Raw: pos.Raw}
	}
	pos.DirectoryID = key
	return observer, nil
}

func (pos *tailPosition) readState(observer *jobStateObserver) error {
	if err := observer.read(); err != nil {
		return err
	}
	if observer.s.State == "" {
		return os.ErrNotExist
	}
	if pos.DirectoryID == "" {
		pos.DirectoryID = observer.identity.key
	}
	pos.StateCursor = observer.cursor.String()
	pos.RunID = observer.s.RunID
	return nil
}

// Frame complete records with their successor cursor. A partial SSH frame
// never advances the consumer, including for binary output without newlines.
type frameWriter struct {
	enc     *json.Encoder
	pending bytes.Buffer
	pos     tailPosition
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
	observer, e := pos.openObservation(dir)
	if e != nil {
		return e
	}
	defer closeBestEffort(observer)
	w := frameWriter{enc: json.NewEncoder(os.Stdout), pos: pos}
	for ctx.Err() == nil {
		if e = w.pos.readState(observer); e != nil {
			return e
		}
		s := observer.s
		if w.pos.Mode == "since" {
			w.pos.Mode = "seq"
			if w.pos.Raw && s.Engine == "cmd" {
				w.pos.Mode = "offset"
			}
		}
		if w.pos.Mode == "seq" {
			e = writeJobProgress(&w, dir, uint64(w.pos.Next), s, &observer.identity)
		} else {
			e = writeJobLog(&w, dir, w.pos.Next, s, &observer.identity)
		}
		frame := tailFrame{Position: w.pos}
		if errors.Is(e, errDrained) {
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
			if e = readJSON(path, &pos); e == nil {
				if _, e = parseCursor(pos.StateCursor); e != nil {
					return fmt.Errorf("read follow position: %w", e)
				}
			} else if !os.IsNotExist(e) {
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
			diagnosticf("%s", cleanDiag(diag.Bytes()))
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
	observer, err := pos.openObservation(dir)
	if err != nil {
		return err
	}
	defer closeBestEffort(observer)
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
		if e := pos.readState(observer); e != nil {
			return e
		}
		drained, e := readMessageEvents(ctx, dir, &pos, &observer.identity, emit)
		if e != nil {
			return e
		}
		frame := tailFrame{Position: pos}
		if drained && observer.s.final {
			frame.Terminal = &observer.s
			return emit(frame)
		}
		if e = emit(frame); e != nil {
			return e
		}
		_ = pause(ctx, time.Second)
	}
	return errObserveTimeout
}

func readMessageEvents(ctx context.Context, dir string, pos *tailPosition, identity *jobIdentity, emit func(tailFrame) error) (bool, error) {
	f, err := openObservedFile(dir, "events.jsonl", identity)
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
			Method     string
			Time       string
			Params     json.RawMessage
			Seq        int64
			TurnNumber int64 `json:"turn_number"`
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
