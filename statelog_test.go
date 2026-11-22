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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var logTestTime = time.Date(2026, 11, 22, 8, 0, 0, 123000000, time.UTC)

func testLogFrame(t *testing.T, seq uint64, kind string, value any) []byte {
	t.Helper()
	r, err := newRecord(kind, value)
	if err != nil {
		t.Fatal(err)
	}
	r.Seq, r.Time = seq, logTestTime
	frame, err := encodeFrame(r)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func testLogWrite(t *testing.T, f *os.File, frame []byte) {
	t.Helper()
	if err := writeFrame(f.Write, frame); err != nil {
		t.Fatal(err)
	}
}

func testLogSegment(t *testing.T, dir string, seg uint64) *os.File {
	t.Helper()
	stateDir := filepath.Join(dir, "state")
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	f, err := createSegment(stateDir, seg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func openTestLogWriter(t *testing.T, dir string) (*stateWriter, func()) {
	t.Helper()
	lock, err := lockPath(filepath.Join(dir, "owner.lock"), os.O_CREATE)
	if err != nil || lock == nil {
		t.Fatalf("lock owner: %v, %v", lock, err)
	}
	w, err := openStateLog(dir)
	if err != nil {
		_ = lock.Close()
		t.Fatal(err)
	}
	closeWriter := func() {
		t.Helper()
		if err := w.Close(); err != nil {
			t.Error(err)
		}
		if lock != nil {
			if err := lock.Close(); err != nil {
				t.Error(err)
			}
			lock = nil
		}
	}
	t.Cleanup(closeWriter)
	return w, closeWriter
}

func testLogLatest(t *testing.T, dir, kind string, seq, seg uint64) stateCursor {
	t.Helper()
	r, c, err := latestStateKind(dir, kind)
	if err != nil || r.Seq != seq || c.Seq != seq || c.Seg != seg || (seq != 0 && c.Off == 0) {
		t.Fatalf("latestStateKind(%q) = seq %d, cursor %v, err %v; want seq %d, seg %d", kind, r.Seq, c, err, seq, seg)
	}
	return c
}

func testLogNext(t *testing.T, tail *stateTail, seqs ...uint64) []stateRecord {
	t.Helper()
	records, err := tail.Next()
	if err != nil {
		t.Fatal(err)
	}
	var got []uint64
	for _, r := range records {
		got = append(got, r.Seq)
	}
	if !reflect.DeepEqual(got, seqs) {
		t.Fatalf("stateTail.Next() sequences = %v; want %v", got, seqs)
	}
	return records
}

func TestStateLogFrame(t *testing.T) {
	want := `{"seq":1,"t":"2026-11-22T08:00:00.123Z","k":"status","v":null,"crc":"f43f13da"}` + "\n"
	if got := testLogFrame(t, 1, "status", nil); string(got) != want {
		t.Fatalf("frame = %q, want %q", got, want)
	}
	value := `a literal ,"crc":" and a newline` + "\n" + strings.Repeat("x", 3*latestWindow)
	line := testLogFrame(t, 1, "status", value)
	if !json.Valid(line) || !bytes.HasSuffix(line, []byte("\"}\n")) {
		t.Fatal("frame is not a newline-terminated JSON object")
	}
	record, ok := decodeFrame(line)
	var got string
	if !ok || json.Unmarshal(record.Value, &got) != nil || got != value || record.Time != logTestTime {
		t.Fatal("frame did not round trip")
	}
	// Put the exact CRC field marker in nested JSON. The payload also
	// contains its escaped spelling as part of a string.
	nested := testLogFrame(t, 1, "status", json.RawMessage(`{"v":"text","crc":"inner"}`))
	if bytes.Count(nested, crcField) != 2 {
		t.Fatal("nested fixture does not exercise the last CRC marker")
	}
	if _, ok := decodeFrame(nested); !ok {
		t.Fatal("nested CRC field masked the envelope CRC")
	}
	badCRC := bytes.Clone(line)
	badCRC[bytes.LastIndex(badCRC, []byte("xxxx"))] = 'y'
	for _, tc := range []struct {
		name string
		line []byte
	}{
		{"bad CRC", badCRC},
		{"missing newline", line[:len(line)-1]},
		{"zero padding", append(bytes.Clone(line[:len(line)-1]), make([]byte, 10)...)},
		{"empty", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, ok := decodeFrame(tc.line); ok {
				t.Fatal("invalid frame accepted")
			}
		})
	}

	dir := t.TempDir()
	seg := testLogSegment(t, dir, 1)
	testLogWrite(t, seg, line)
	testLogWrite(t, seg, testLogFrame(t, 2, "config", "new"))
	testLogLatest(t, dir, "status", 1, 1)
	testLogLatest(t, dir, "config", 2, 1)
	tail, err := openStateTail(dir, stateCursor{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(tail)
	testLogNext(t, tail, 1, 2)
}

func TestStateLogPrefixAndRecovery(t *testing.T) {
	for _, tc := range []struct {
		name string
		bad  func(*testing.T) []byte
	}{
		{"checksum", func(t *testing.T) []byte {
			f := testLogFrame(t, 2, "status", "bad")
			f[bytes.Index(f, []byte("bad"))] = 'p'
			return f
		}},
		{"sequence gap", func(t *testing.T) []byte { return testLogFrame(t, 3, "status", nil) }},
		{"duplicate sequence", func(t *testing.T) []byte { return testLogFrame(t, 1, "status", nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			seg := testLogSegment(t, dir, 1)
			testLogWrite(t, seg, testLogFrame(t, 1, "status", "good"))
			testLogWrite(t, seg, tc.bad(t))
			testLogWrite(t, seg, testLogFrame(t, 4, "status", "independently valid"))
			c := testLogLatest(t, dir, "status", 4, 1)
			tail, err := openStateTail(dir, stateCursor{})
			if err != nil {
				t.Fatal(err)
			}
			defer closeBestEffort(tail)
			testLogNext(t, tail, 1)
			testLogNext(t, tail)
			if err := seg.Close(); err != nil {
				t.Fatal(err)
			}
			w, _ := openTestLogWriter(t, dir)
			if w.RecoveryError() == nil {
				t.Fatal("mid-segment corruption was not reported")
			}
			if err := w.Append("status", "after damage"); err != nil {
				t.Fatal(err)
			}
			testLogLatest(t, dir, "status", 5, 2)
			testLogNext(t, tail, 5)
			resumed, err := openStateTail(dir, c)
			if err != nil {
				t.Fatal(err)
			}
			defer closeBestEffort(resumed)
			testLogNext(t, resumed, 5)
		})
	}
}

func TestStateLogLatestFallbackAndPartialTail(t *testing.T) {
	dir := t.TempDir()
	testLogLatest(t, dir, "status", 0, 0)
	first := testLogSegment(t, dir, 1)
	testLogWrite(t, first, testLogFrame(t, 1, "config", "initial"))
	testLogWrite(t, first, testLogFrame(t, 2, "status", "initial"))
	c := testLogLatest(t, dir, "status", 2, 1)
	second := testLogSegment(t, dir, 2)
	testLogLatest(t, dir, "status", 2, 1)

	tail, err := openStateTail(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(tail)
	frame := testLogFrame(t, 3, "status", strings.Repeat("x", 2*latestWindow))
	testLogWrite(t, second, frame[:len(frame)-1])
	testLogLatest(t, dir, "status", 2, 1)
	testLogLatest(t, dir, "config", 1, 1)
	testLogNext(t, tail)
	if tail.Cursor() != c {
		t.Fatal("partial record advanced cursor")
	}
	testLogWrite(t, second, frame[len(frame)-1:])
	testLogLatest(t, dir, "status", 3, 2)
	testLogNext(t, tail, 3)

	testLogWrite(t, second, make([]byte, 2*latestWindow))
	third := testLogSegment(t, dir, 3)
	testLogLatest(t, dir, "status", 3, 2)
	testLogWrite(t, third, testLogFrame(t, 4, "status", nil))
	testLogNext(t, tail, 4)
}

func TestStateLogCursor(t *testing.T) {
	for _, want := range []stateCursor{{}, {Off: 91, Seq: 1}, {Seg: 42, Off: 2491, Seq: ^uint64(0)}} {
		got, err := parseCursor(want.String())
		if err != nil || got != want {
			t.Fatalf("parseCursor(%q) = %v, %v", want.String(), got, err)
		}
	}
	for _, bad := range []string{"", "1:2", "1:2:3:4", "1:-2:3", "1:2:x", "1:2:18446744073709551616"} {
		if _, err := parseCursor(bad); err == nil {
			t.Fatalf("parseCursor(%q) succeeded", bad)
		}
	}
}

func TestStateLogRotateAndRestart(t *testing.T) {
	dir := t.TempDir()
	w, closeWriter := openTestLogWriter(t, dir)
	if err := w.Append("config", "first"); err != nil {
		t.Fatal(err)
	}
	if err := w.Append("status", "first"); err != nil {
		t.Fatal(err)
	}
	if err := w.Rotate("ignored", "ignored"); err != nil || w.NeedsRotate() {
		t.Fatalf("rotated below threshold: %v", err)
	}
	c := testLogLatest(t, dir, "status", 2, 1)
	tail, err := openStateTail(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(tail)
	w.SetRotateBytes(1)
	if err := w.Rotate("first", "first", stateRecord{Kind: "inbox_ack", Value: json.RawMessage(`{"seq":8}`)}); err != nil {
		t.Fatal(err)
	}
	got := testLogNext(t, tail, 3, 4, 5, 6)
	if got[0].Kind != "seal" || string(got[0].Value) != `{"next":2}` || got[1].Kind != "config" ||
		got[2].Kind != "inbox_ack" || got[3].Kind != "status" {
		t.Fatalf("rotation records = %+v", got)
	}
	if w.NeedsRotate() {
		t.Fatal("checkpoint snapshot alone triggered another rotation")
	}
	if err := w.Append("status", "second"); err != nil {
		t.Fatal(err)
	}
	if err := w.Rotate("second", "second"); err != nil {
		t.Fatal(err)
	}
	testLogNext(t, tail, 7, 8, 9, 10)
	segments, err := logSegments(filepath.Join(dir, "state"))
	if err != nil || !reflect.DeepEqual(segments, []uint64{2, 3}) {
		t.Fatalf("retained segments = %v, %v", segments, err)
	}
	oldCursorTail, err := openStateTail(dir, c)
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(oldCursorTail)
	testLogNext(t, oldCursorTail, 4, 5, 6, 7, 8, 9, 10)
	closeWriter()
	w, closeWriter = openTestLogWriter(t, dir)
	if err := w.Final("done"); err != nil {
		t.Fatal(err)
	}
	got = testLogNext(t, tail, 11)
	if !got[0].Final || got[0].Kind != "status" {
		t.Fatalf("Final returned %+v", got[0])
	}
	testLogLatest(t, dir, "status", 11, 5)
	closeWriter()
}

func TestStateLogInterruptedSeal(t *testing.T) {
	dir := t.TempDir()
	w, closeWriter := openTestLogWriter(t, dir)
	if err := w.Append("status", "before rotation"); err != nil {
		t.Fatal(err)
	}
	w.SetRotateBytes(1)
	w.appendFn = func(f *os.File, line []byte) (int, error) {
		if f == w.file {
			return f.Write(line[:len(line)-1])
		}
		return f.Write(line)
	}
	if err := w.Rotate("config", "new snapshot"); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("interrupted seal: %v", err)
	}
	if err := w.Append("status", "must stop after short write"); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writer accepted an append after short write: %v", err)
	}
	closeWriter()
	tail, err := openStateTail(dir, stateCursor{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(tail)
	testLogLatest(t, dir, "status", 4, 2)
	testLogNext(t, tail, 1, 3, 4)
	w, _ = openTestLogWriter(t, dir)
	if err := w.Append("status", "after restart"); err != nil {
		t.Fatal(err)
	}
	testLogNext(t, tail, 5)
}

func TestStateLogCrashAtEveryFrameByte(t *testing.T) {
	if dir := os.Getenv("CX_TEST_STATELOG_CRASH_DIR"); dir != "" {
		k, err := strconv.Atoi(os.Getenv("CX_TEST_STATELOG_CRASH_BYTES"))
		if err != nil {
			t.Fatal(err)
		}
		w, _ := openTestLogWriter(t, dir)
		if err := w.Append("status", "before crash"); err != nil {
			t.Fatal(err)
		}
		w.appendFn = func(f *os.File, frame []byte) (int, error) {
			r, ok := decodeFrame(frame)
			if !ok {
				t.Fatal("invalid writer frame")
			}
			r.Time = logTestTime // Fix the width for exhaustive byte offsets.
			frame, err := encodeFrame(r)
			if err != nil || k > len(frame) {
				t.Fatalf("invalid crash offset %d: %v", k, err)
			}
			testLogWrite(t, f, frame[:k])
			os.Exit(42)
			return 0, nil
		}
		_ = w.Append("status", "crash")
		t.Fatal("crash hook did not run")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	size := len(testLogFrame(t, 3, "status", "crash"))
	for k := 0; k <= size; k++ {
		t.Run(strconv.Itoa(k), func(t *testing.T) {
			dir := t.TempDir()
			w, closeWriter := openTestLogWriter(t, dir)
			if err := w.Append("status", "before restart"); err != nil {
				t.Fatal(err)
			}
			closeWriter()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStateLogCrashAtEveryFrameByte$", "-test.timeout=5s")
			cmd.Env = append(os.Environ(), "CX_TEST_STATELOG_CRASH_DIR="+dir, "CX_TEST_STATELOG_CRASH_BYTES="+strconv.Itoa(k))
			out, err := cmd.CombinedOutput()
			var ex *exec.ExitError
			if !errors.As(err, &ex) || ex.ExitCode() != 42 {
				t.Fatalf("fault injection exited with %v: %s", err, out)
			}
			seq := uint64(2)
			if k == size {
				seq = 3
			}
			testLogLatest(t, dir, "status", seq, 2)
			tail, err := openStateTail(dir, stateCursor{})
			if err != nil {
				t.Fatal(err)
			}
			defer closeBestEffort(tail)
			if k == size {
				testLogNext(t, tail, 1, 2, 3)
			} else {
				testLogNext(t, tail, 1, 2)
			}
			w, _ = openTestLogWriter(t, dir)
			if w.RecoveryError() != nil {
				t.Fatal(w.RecoveryError())
			}
			if err := w.Append("status", "recovered"); err != nil {
				t.Fatal(err)
			}
			testLogLatest(t, dir, "status", seq+1, 3)
			testLogNext(t, tail, seq+1)
		})
	}
}

func TestStateLogConcurrentReaders(t *testing.T) {
	dir := t.TempDir()
	w, _ := openTestLogWriter(t, dir)
	w.SetRotateBytes(4096)
	if err := w.Append("status", 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			if err := pollTestLog(ctx, dir, i%2 == 0); err != nil {
				t.Error(err)
			}
		})
	}
	until := time.Now().Add(2 * time.Second)
	for i := 1; time.Now().Before(until); i++ {
		if err := w.Append("status", i); err != nil {
			t.Fatal(err)
		}
		if w.NeedsRotate() {
			if err := w.Rotate("config", i); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Millisecond)
	}
	if err := w.Final(0); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func pollTestLog(ctx context.Context, dir string, useTail bool) error {
	var read func() ([]stateRecord, error)
	if useTail {
		tail, err := openStateTail(dir, stateCursor{})
		if err != nil {
			return err
		}
		defer closeBestEffort(tail)
		read = tail.Next
	} else {
		read = func() ([]stateRecord, error) {
			r, _, err := latestState(dir)
			return []stateRecord{r}, err
		}
	}
	var seq uint64
	for {
		records, err := read()
		if err != nil {
			return err
		}
		for _, r := range records {
			if r.Seq == 0 && !useTail {
				// A read racing two rotations can lose every listed path.
				continue
			}
			if r.Seq < seq || (useTail && r.Seq == seq) {
				return fmt.Errorf("sequence regressed from %d to %d", seq, r.Seq)
			}
			seq = r.Seq
			if r.Final {
				return nil
			}
		}
		if err := pause(ctx, time.Millisecond); err != nil {
			return err
		}
	}
}

func TestMailboxConcurrentAppend(t *testing.T) {
	const count = 20
	if dir := os.Getenv("CX_TEST_STATELOG_APPEND_DIR"); dir != "" {
		for i := range count {
			_, err := appendJobMailbox(t.Context(), dir, "inbox", nil, stateRecord{
				ID: strconv.Itoa(i), Kind: "msg", Value: json.RawMessage(`{"text":"test"}`),
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	dir := fixture(t, "waiting")
	path := filepath.Join(dir, "inbox")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestMailboxConcurrentAppend$", "-test.timeout=10s")
			cmd.Env = append(os.Environ(), "CX_TEST_STATELOG_APPEND_DIR="+dir)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("mailbox append child: %v: %s", err, out)
			}
		})
	}
	wg.Wait()
	tail, err := openSegmentedTail(path, stateCursor{})
	if err != nil {
		t.Fatal(err)
	}
	defer closeBestEffort(tail)
	seen := make(map[string]bool)
	records, err := tail.Next()
	if err != nil || len(records) != count {
		t.Fatalf("segmented tail: %d records, %v", len(records), err)
	}
	for i, r := range records {
		if r.Seq != uint64(i+1) || r.ID == "" || seen[r.ID] {
			t.Fatalf("invalid append: %+v", r)
		}
		seen[r.ID] = true
	}
	r, c, err := latestFrame(segmentPath(path, 1), 1, "msg")
	if err != nil || r.Seq != count || c != tail.Cursor() {
		t.Fatalf("latestFileKind = %+v, %v, %v", r, c, err)
	}
	r, err = appendJobMailbox(t.Context(), dir, "inbox", nil, stateRecord{ID: records[0].ID, Kind: "msg", Value: records[0].Value}, nil)
	if err != nil || r.Seq != 1 {
		t.Fatalf("idempotent retry: %+v, %v", r, err)
	}
	if _, err := appendJobMailbox(t.Context(), dir, "inbox", nil, stateRecord{ID: r.ID, Kind: "msg", Value: json.RawMessage(`"different"`)}, nil); err == nil {
		t.Fatal("conflicting duplicate was accepted")
	}

	f, err := openJobFile(segmentPath(path, 1), jobFileAppend)
	if err != nil {
		t.Fatal(err)
	}
	testLogWrite(t, f, []byte(`{"seq":`))
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = appendJobMailbox(t.Context(), dir, "inbox", nil, stateRecord{ID: "new", Kind: "msg", Value: json.RawMessage(`{}`)}, nil)
	if err != nil || r.Seq != count+1 {
		t.Fatalf("append after torn tail = %+v, %v", r, err)
	}
	testLogNext(t, tail, count+1)
	if c := tail.Cursor(); c.Seg != 2 {
		t.Fatalf("tail stayed on torn segment: %v", c)
	}
}
