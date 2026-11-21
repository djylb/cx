package main

import (
	"bytes"
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
	Mode string `json:"mode"`
	Next int64  `json:"next"`
	Raw  bool   `json:"raw"`
}
type tailFrame struct {
	Data     []byte       `json:"data,omitempty"`
	Position tailPosition `json:"position"`
	Terminal *status      `json:"terminal,omitempty"`
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
	path := ""
	if o.cursor != "" {
		p, j, ok := strings.Cut(id, "/")
		if !ok {
			p, j = "flat", p
		}
		path, e = cursorFile("tail", name, p, j, strconv.FormatBool(o.raw), o.cursor)
		if e != nil {
			return e
		}
		lock, e := lockUntil(ctx, path+".lock")
		if e != nil {
			return e
		}
		defer lock.Close()
		if o.since == "" {
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
		dec := json.NewDecoder(rd)
		var terminal *status
		for {
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
		rd.Close()
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
