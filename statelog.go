package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defaultRotateBytes = 8 << 20
	latestWindow       = 64 << 10
	maxSegment         = 999999
)

type jobFileMode uint8

const (
	jobFileRead jobFileMode = iota
	jobFileNew
	jobFileAppend
	jobFileLock
	jobFileAppendCreate
	jobFileLockExisting
	jobFileDirectory
	jobFileDirectoryID
	jobFileSync
)

var (
	frameCRC = crc32.MakeTable(crc32.Castagnoli)
	crcField = []byte(`,"crc":"`)
)

// stateRecord is a verified log frame. Value is the original JSON payload.
type stateRecord struct {
	Time       time.Time       `json:"t"`
	Kind       string          `json:"k"`
	ID         string          `json:"id,omitempty"`
	Value      json.RawMessage `json:"v"`
	position   stateCursor
	Seq        uint64 `json:"seq"`
	frameBytes uint64
	Final      bool `json:"final,omitempty"`
}

// stateCursor points just after a delivered record. The zero value starts at
// the oldest available segment.
type stateCursor struct{ Seg, Off, Seq uint64 }

func (c stateCursor) String() string {
	return fmt.Sprintf("%d:%d:%d", c.Seg, c.Off, c.Seq)
}

// parseCursor decodes the decimal seg:off:seq representation.
func parseCursor(s string) (stateCursor, error) {
	fields := strings.Split(s, ":")
	var c stateCursor
	if len(fields) == 3 {
		for i, value := range []*uint64{&c.Seg, &c.Off, &c.Seq} {
			n, err := strconv.ParseUint(fields[i], 10, 64)
			if err != nil {
				return stateCursor{}, fmt.Errorf("invalid log cursor %q", s)
			}
			*value = n
		}
		return c, nil
	}
	return stateCursor{}, fmt.Errorf("invalid log cursor %q", s)
}

func encodeFrame(r stateRecord) ([]byte, error) {
	if r.Seq == 0 || r.Kind == "" {
		return nil, errors.New("log record requires a sequence number and kind")
	}
	// Keep the checksummed frame encoding independent of the in-memory layout.
	type recordFields stateRecord
	wire := struct {
		Seq *uint64 `json:"seq"`
		*recordFields
		ID    *string `json:"id,omitempty"`
		Final *bool   `json:"final,omitempty"`
	}{Seq: &r.Seq, recordFields: (*recordFields)(&r)}
	if r.ID != "" {
		wire.ID = &r.ID
	}
	if r.Final {
		wire.Final = &r.Final
	}
	line, err := json.Marshal(wire)
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-1]
	checksum := crc32.Checksum(line, frameCRC)
	return fmt.Appendf(line, ",\"crc\":\"%08x\"}\n", checksum), nil
}

func decodeFrame(line []byte) (stateRecord, bool) {
	var r stateRecord
	split := bytes.LastIndex(line, crcField)
	if len(line) == 0 || line[0] != '{' || split < 0 {
		return r, false
	}
	suffix := line[split+len(crcField):]
	if len(suffix) != 11 || string(suffix[8:]) != "\"}\n" {
		return r, false
	}
	checksum, err := strconv.ParseUint(string(suffix[:8]), 16, 32)
	if err != nil || uint32(checksum) != crc32.Checksum(line[:split], frameCRC) {
		return r, false
	}
	if err := json.Unmarshal(line, &r); err != nil || r.Seq == 0 || r.Time.IsZero() || r.Kind == "" || r.Value == nil {
		return stateRecord{}, false
	}
	return r, true
}

func newRecord(kind string, value any) (stateRecord, error) {
	data, err := json.Marshal(value)
	return stateRecord{Kind: kind, Time: time.Now().UTC(), Value: data}, err
}

func writeFrame(write func([]byte) (int, error), line []byte) error {
	n, err := write(line)
	if err == nil && n != len(line) {
		return io.ErrShortWrite
	}
	return err
}

func segmentPath(dir string, seg uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.jsonl", seg))
}

// ReadDir sorts by filename, which is also sequence order for six digits.
func logSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var segments []uint64
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || len(name) != len("000001.jsonl") || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		n, err := strconv.ParseUint(name[:6], 10, 64)
		if err == nil && n != 0 {
			segments = append(segments, n)
		}
	}
	return segments, nil
}

// stateWriter publishes one job's state log. Under I1, the caller must hold
// owner.lock from Open through Close and serialize all calls to the stateWriter.
type stateWriter struct {
	recoveryErr  error
	failed       error
	file         *os.File
	appendFn     func(*os.File, []byte) (int, error)
	syncFn       func(*os.File) error
	closeFn      func(*os.File) error
	syncDirFn    func(string) error
	dir          string
	seg, seq     uint64
	size         uint64
	snapshotSize uint64
	rotateBytes  uint64
}

// Open creates a fresh segment, even when the previous segment has no records.
// The caller publishes the initial config and status with Append.
func openStateLog(dir string) (*stateWriter, error) {
	return openLogWriter(dir, false)
}

// A deduplication claim is an independent snapshot. Only its newest segment
// can affect the next claim. Job and checkout logs can depend on older data.
func openLogWriter(dir string, snapshotOnly bool) (*stateWriter, error) {
	parent := dir
	if err := os.MkdirAll(parent, 0700); err != nil {
		return nil, err
	}
	dir = filepath.Join(parent, "state")
	if err := os.Mkdir(dir, 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}
	// An interrupted earlier creation may have left a visible but unsynced entry.
	if err := syncJobDir(parent); err != nil {
		return nil, err
	}
	segments, err := logSegments(dir)
	if err != nil {
		return nil, err
	}
	w := &stateWriter{dir: dir, seg: 1, rotateBytes: defaultRotateBytes, appendFn: (*os.File).Write, syncFn: syncJobFile, closeFn: (*os.File).Close, syncDirFn: syncJobDir}
	if len(segments) > 0 {
		w.seg = segments[len(segments)-1] + 1
		if snapshotOnly {
			for i := len(segments) - 1; i >= 0 && w.seq == 0; i-- {
				_, cursor, readErr := latestFrame(segmentPath(dir, segments[i]), segments[i], "claim")
				if readErr != nil {
					return nil, readErr
				}
				w.seq = cursor.Seq
			}
		} else {
			w.seq, w.recoveryErr, err = recoverLog(dir, segments, false)
			if err != nil {
				return nil, err
			}
		}
	}
	w.file, err = createSegment(dir, w.seg)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func createSegment(dir string, seg uint64) (*os.File, error) {
	if seg > maxSegment {
		return nil, errors.New("state log exhausted its six-digit segment numbers")
	}
	return openJobFile(segmentPath(dir, seg), jobFileNew)
}

// RecoveryError reports detected damage preceding an otherwise valid frame.
// The caller can log the diagnostic after Open succeeds.
func (w *stateWriter) RecoveryError() error { return w.recoveryErr }

// SetRotateBytes sets the size threshold. Zero restores the default (8 MiB).
func (w *stateWriter) SetRotateBytes(n uint64) {
	if n == 0 {
		n = defaultRotateBytes
	}
	w.rotateBytes = n
}

// NeedsRotate reports whether the caller should prepare a rotation snapshot.
func (w *stateWriter) NeedsRotate() bool {
	return w.size > w.rotateBytes && w.size-w.snapshotSize > min(w.snapshotSize, w.rotateBytes)
}

func (w *stateWriter) writable() error {
	if w.file == nil {
		return os.ErrClosed
	}
	return w.failed
}

// Append publishes one complete frame in a single Write.
func (w *stateWriter) Append(kind string, v any) error {
	if kind == "seal" {
		return errors.New("seal records are written by Rotate")
	}
	return w.append(kind, v)
}

// Final stages a candidate outside the numbered namespace. Only a candidate
// that has been flushed and closed may be linked as the next log segment.
func (w *stateWriter) Final(v any, outcomes ...stateRecord) error {
	record, err := newRecord("status", v)
	if err != nil {
		return err
	}
	record.Final = true
	return w.commit(append(outcomes, record)...)
}

// commit publishes one pre-synced immutable segment with a single link.
func (w *stateWriter) commit(outcomes ...stateRecord) error {
	if err := w.writable(); err != nil {
		return err
	}
	if w.seg >= maxSegment {
		return errors.New("state log exhausted its segment numbers")
	}
	if uint64(len(outcomes)) > math.MaxUint64-w.seq {
		return errors.New("state log sequence exhausted")
	}
	for _, record := range outcomes {
		if record.Kind == "blob" {
			var ref blobRef
			if err := json.Unmarshal(record.Value, &ref); err != nil {
				return err
			}
			if ref.Role == preparedBlobRole {
				if err := syncPreparedCheckout(filepath.Dir(w.dir), w.syncFn, w.syncDirFn); err != nil {
					return err
				}
			}
		}
	}
	if err := w.syncFn(w.file); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	f, err := createJobTemp(w.dir, ".commit-")
	if err != nil {
		return err
	}
	defer removeBestEffort(f.Name())
	for i, outcome := range outcomes {
		outcome.Seq = w.seq + uint64(i) + 1
		line, encodeErr := encodeFrame(outcome)
		if encodeErr != nil {
			err = encodeErr
			break
		}
		if err = w.write(f, line); err != nil {
			break
		}
	}
	if err == nil {
		err = w.syncFn(f)
	}
	if err = errors.Join(err, w.closeFn(f)); err != nil {
		return err
	}
	if err = w.syncDirFn(w.dir); err != nil {
		return err
	}
	if err = os.Link(f.Name(), segmentPath(w.dir, w.seg+1)); err != nil {
		return err
	}
	return w.syncDirFn(w.dir)
}

// Recovering ownership does not imply that the previous owner's writes
// reached disk. A final status can still refer to records in those segments.
func (w *stateWriter) syncHistory() error {
	segments, err := logSegments(w.dir)
	if err != nil {
		return err
	}
	for _, seg := range segments {
		if seg == w.seg {
			break
		}
		if err := syncHistoricalSegment(segmentPath(w.dir, seg), w.syncFn); err != nil {
			return err
		}
	}
	return w.syncDirFn(w.dir)
}

var errStateHistoryBusy = errors.New("historical state segment is held without write sharing")

func syncHistoricalSegment(path string, sync func(*os.File) error) error {
	err := syncExistingJobFile(path, sync)
	if stateHistorySharingConflict(err) {
		return fmt.Errorf("%w: %s: %w; retry after the reader releases the file", errStateHistoryBusy, path, err)
	}
	if err != nil {
		return fmt.Errorf("sync historical state segment %s: %w", path, err)
	}
	return nil
}

func (w *stateWriter) sync() error {
	if err := w.writable(); err != nil {
		return err
	}
	if err := w.syncFn(w.file); err != nil {
		w.failed = err
		return err
	}
	return w.syncDirFn(w.dir)
}

func (w *stateWriter) append(kind string, v any) error {
	if err := w.writable(); err != nil {
		return err
	}
	r, err := newRecord(kind, v)
	if err != nil {
		return err
	}
	r.Seq = w.seq + 1
	line, err := encodeFrame(r)
	if err != nil {
		return err
	}
	if err = w.write(w.file, line); err != nil {
		return err
	}
	w.seq = r.Seq
	w.size += uint64(len(line))
	return nil
}

func (w *stateWriter) write(file *os.File, line []byte) error {
	w.failed = writeFrame(func(line []byte) (int, error) { return w.appendFn(file, line) }, line)
	return w.failed
}

type segmentSeal struct {
	Next uint64 `json:"next"`
}

// Rotate publishes config, optional checkpoint records, and a full status in
// a new segment once the threshold is exceeded. It writes status last within
// the snapshot and seals the previous segment only after the snapshot is
// complete. A crash before the seal can leave one reserved sequence missing
// across the segment boundary.
func (w *stateWriter) Rotate(config, status any, checkpoints ...stateRecord) error {
	if err := w.writable(); err != nil {
		return err
	}
	if !w.NeedsRotate() {
		return nil
	}
	if uint64(len(checkpoints))+3 > math.MaxUint64-w.seq {
		return errors.New("state log sequence exhausted")
	}
	configRecord, err := newRecord("config", config)
	if err != nil {
		return err
	}
	statusRecord, err := newRecord("status", status)
	if err != nil {
		return err
	}
	snapshot := make([]stateRecord, 0, len(checkpoints)+2)
	snapshot = append(snapshot, configRecord)
	snapshot = append(snapshot, checkpoints...)
	snapshot = append(snapshot, statusRecord)

	nextSeg := w.seg + 1
	seal, err := newRecord("seal", segmentSeal{Next: nextSeg})
	if err != nil {
		return err
	}
	seal.Seq = w.seq + 1
	sealLine, err := encodeFrame(seal)
	if err != nil {
		return err
	}
	lines := make([][]byte, 0, len(snapshot))
	var size uint64
	for i, r := range snapshot {
		if i > 0 && i < len(snapshot)-1 && (r.Kind == "seal" || r.Kind == "config" || r.Kind == "status") {
			return fmt.Errorf("rotation checkpoint cannot have kind %q", r.Kind)
		}
		r.Seq = seal.Seq + uint64(i) + 1
		r.Time = seal.Time
		line, encodeErr := encodeFrame(r)
		if encodeErr != nil {
			return encodeErr
		}
		lines = append(lines, line)
		size += uint64(len(line))
	}

	if nextSeg > maxSegment {
		return errors.New("state log exhausted its six-digit segment numbers")
	}
	// Once the checkpoint can survive a crash, its predecessor must also
	// survive through the sequence immediately before the reserved seal.
	if syncErr := w.sync(); syncErr != nil {
		return syncErr
	}
	next, err := createJobTemp(w.dir, ".rotate-")
	if err != nil {
		return err
	}
	defer removeBestEffort(next.Name())
	for _, line := range lines {
		if err = w.write(next, line); err != nil {
			_ = next.Close()
			return err
		}
	}
	if err = w.syncFn(next); err == nil {
		err = os.Link(next.Name(), segmentPath(w.dir, nextSeg))
	}
	if err == nil {
		err = w.syncDirFn(w.dir)
	}
	if err != nil {
		_ = next.Close()
		w.failed = err
		return err
	}
	if err = w.write(w.file, sealLine); err != nil {
		_ = next.Close()
		return err
	}
	old := w.file
	w.file, w.seg, w.seq, w.size = next, nextSeg, seal.Seq+uint64(len(lines)), size
	w.snapshotSize = size
	err = old.Close()
	if segments, listErr := logSegments(w.dir); listErr == nil {
		for _, seg := range segments {
			if seg+1 < w.seg {
				removeJobSegment(segmentPath(w.dir, seg))
			}
		}
	}
	return err
}

// Close releases the segment handle. Final is responsible for flushing it.
func (w *stateWriter) Close() error {
	if w.file == nil {
		return nil
	}
	err := w.closeFn(w.file)
	w.file = nil
	return err
}

// Latest finds the newest independently verified status snapshot. A zero
// stateRecord with no error means that no status has been published yet.
func latestState(dir string) (stateRecord, stateCursor, error) {
	return latestStateKind(dir, "status")
}

// latestStateKind finds the newest verified frame of kind, searching backward
// through the retained segments. An empty kind matches every frame.
func latestStateKind(dir, kind string) (stateRecord, stateCursor, error) {
	dir = filepath.Join(dir, "state")
	for attempt := range 2 {
		segments, err := logSegments(dir)
		if err != nil {
			return stateRecord{}, stateCursor{}, err
		}
		var reclaimed bool
		for i := len(segments) - 1; i >= 0; i-- {
			r, c, err := latestFrame(segmentPath(dir, segments[i]), segments[i], kind)
			if os.IsNotExist(err) {
				reclaimed = true
				continue
			}
			if err != nil {
				return r, c, err
			}
			if r.Final {
				if err := syncJobDir(dir); err != nil {
					return stateRecord{}, stateCursor{}, err
				}
			}
			if r.Seq != 0 {
				if reclaimed && attempt == 0 {
					break
				}
				return r, c, nil
			}
		}
		if !reclaimed {
			break
		}
		// A newer listed segment disappeared. Refresh the listing once.
	}
	return stateRecord{}, stateCursor{}, nil
}

// latestFileKind finds a verified frame in a single-file log.
func latestFileKind(path, kind string) (stateRecord, stateCursor, error) {
	r, c, err := latestFrame(path, 0, kind)
	if os.IsNotExist(err) {
		err = nil
	}
	return r, c, err
}

func latestFrame(path string, seg uint64, kind string) (stateRecord, stateCursor, error) {
	f, err := openJobFile(path, jobFileRead)
	if err != nil {
		return stateRecord{}, stateCursor{}, err
	}
	defer closeBestEffort(f)
	info, err := f.Stat()
	if err != nil {
		return stateRecord{}, stateCursor{}, err
	}
	// Status snapshots are normally <= 64 KiB at <= 10 updates/s, on local
	// disks. Geometric windows also support frames larger than one window.
	size := info.Size()
	limit := size
	for window := min(int64(latestWindow), size); window > 0; {
		start := size - window
		data := make([]byte, window)
		if _, err := f.ReadAt(data, start); err != nil {
			return stateRecord{}, stateCursor{}, err
		}
		for end := int(limit - start); end > 0; {
			end = bytes.LastIndexByte(data[:end], '\n') + 1
			if end == 0 {
				break
			}
			before := bytes.LastIndexByte(data[:end-1], '\n') + 1
			if before == 0 && start != 0 {
				break
			}
			if r, ok := decodeFrame(data[before:end]); ok && (kind == "" || r.Kind == kind) {
				return r, stateCursor{Seg: seg, Off: uint64(start) + uint64(end), Seq: r.Seq}, nil
			}
			limit = start + int64(before)
			end = before
		}
		if start == 0 {
			break
		}
		window += min(window, start)
	}
	return stateRecord{}, stateCursor{}, nil
}

// frameReader retains an incomplete line between reads, without limiting
// frame size or reading bytes already parsed on the common polling path.
type frameReader struct {
	reader  *bufio.Reader
	partial []byte
}

func readFrames(f *os.File, off uint64) frameReader {
	return frameReader{reader: bufio.NewReader(io.NewSectionReader(f, int64(off), math.MaxInt64-int64(off)))}
}

func (r *frameReader) line() ([]byte, error) {
	for {
		chunk, err := r.reader.ReadSlice('\n')
		if err == nil {
			if len(r.partial) == 0 {
				return chunk, nil
			}
			line := append(r.partial, chunk...)
			r.partial = nil
			return line, nil
		}
		r.partial = append(r.partial, chunk...)
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err
		}
	}
}

var errLogDamage = errors.New("damaged log checkpoint")

// Recovery checks adjacent segments together. Rotate can reserve one sequence
// for a seal that never reached disk. Reclaimed state segments can leave holes;
// mailbox segments are never reclaimed.
func recoverLog(dir string, segments []uint64, mailbox bool) (seq uint64, damage error, err error) {
	var previous uint64
	for _, seg := range segments {
		next, bad, err := recoverSegment(segmentPath(dir, seg), seg, seq, previous, mailbox)
		if err != nil {
			return 0, nil, err
		}
		damage = errors.Join(damage, bad)
		seq, previous = next, seg
	}
	return seq, damage, nil
}

func recoverSegment(path string, seg, seq, previous uint64, mailbox bool) (uint64, error, error) {
	f, err := openJobFile(path, jobFileRead)
	if err != nil {
		return 0, nil, err
	}
	defer closeBestEffort(f)
	frames := readFrames(f, 0)
	var off uint64
	var damage error
	if mailbox && seg != previous+1 {
		damage = fmt.Errorf("%w: missing mailbox segment before %s", errLogDamage, path)
	}
	for {
		line, err := frames.line()
		if err == io.EOF {
			return seq, damage, nil
		}
		if err != nil {
			return 0, nil, err
		}
		record, ok := decodeFrame(line)
		consecutive := off != 0 || seg == 1 || mailbox || (previous != 0 && seg == previous+1)
		reservedSeal := !mailbox && off == 0 && previous != 0 && seg == previous+1 &&
			record.Kind == "config" && seq <= math.MaxUint64-2 && record.Seq == seq+2
		if damage == nil && (!ok || record.Seq <= seq || (consecutive && record.Seq != seq+1 && !reservedSeal)) {
			damage = fmt.Errorf("%w: %s at byte %d", errLogDamage, path, off)
		}
		if ok {
			seq = max(seq, record.Seq)
		}
		off += uint64(len(line))
	}
}

func validateSegments(dir string, mailbox bool) error {
	segments, err := logSegments(dir)
	if err != nil {
		return err
	}
	_, damage, err := recoverLog(dir, segments, mailbox)
	return errors.Join(damage, err)
}

// stateTail follows a log from a cursor. The caller serializes Next, stateCursor and Close.
type stateTail struct {
	nextScan time.Time
	damage   error
	file     *os.File
	syncDir  func(string) error
	segments func(string) ([]uint64, error)
	dir      string
	frames   frameReader
	batch    []stateRecord
	pending  []byte
	seg      uint64
	off      uint64
	cursor   stateCursor
	visible  stateCursor
	seal     uint64
	stop     bool
	mailbox  bool
	closed   bool
}

// openStateTail starts following a job's segmented log. A supplied cursor is a
// trusted anchor, even if Latest found it past damage. Segments reclaimed by
// retention are skipped.
func openStateTail(dir string, from stateCursor) (*stateTail, error) {
	return openSegmentedTail(filepath.Join(dir, "state"), from)
}

// Cursor returns the position after the last frame delivered by Next.
func (t *stateTail) Cursor() stateCursor { return t.visible }

// Next returns complete, CRC-valid records up to the current end of the log.
// Within a segment, the first bad frame or sequence gap ends its prefix.
// Across segments, records must advance the sequence, which may have gaps from
// retention, damage, or a successor becoming visible before the old seal.
func (t *stateTail) Next() ([]stateRecord, error) {
	if t.closed {
		return nil, os.ErrClosed
	}
	if t.damage != nil {
		return nil, t.damage
	}
	if err := t.advance(); err != nil {
		return nil, err
	}
	records := t.batch
	t.batch, t.visible = nil, t.cursor
	return records, nil
}

func (t *stateTail) advance() error {
	for {
		if t.file == nil {
			ok, err := t.openNext()
			if err != nil || !ok {
				return err
			}
		}
		if err := t.read(); err != nil {
			return err
		}
		// Discover a successor even if the old writer died before sealing.
		// Once the successor exists, read the old handle again before switching:
		// the first EOF might have raced the old writer's last append.
		next, err := t.successor()
		if err != nil || next == 0 {
			return err
		}
		if err := t.read(); err != nil {
			return err
		}
		_ = t.file.Close()
		t.file, t.seg, t.off, t.seal, t.stop = nil, next, 0, 0, false
	}
}

func (t *stateTail) read() error {
	for !t.stop && t.seal == 0 {
		line := t.pending
		if line == nil {
			var err error
			line, err = t.frames.line()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
		}
		r, ok := decodeFrame(line)
		continuing := t.mailbox || t.off != 0 || t.seg == 1
		if !ok || r.Seq <= t.cursor.Seq || (continuing && r.Seq != t.cursor.Seq+1) {
			if t.mailbox {
				return t.damaged("invalid frame or sequence")
			}
			t.stop = true
			break
		}
		if r.Final {
			if err := t.syncDir(t.dir); err != nil {
				t.pending = line
				return err
			}
		}
		t.pending = nil
		t.off += uint64(len(line))
		t.cursor = stateCursor{Seg: t.seg, Off: t.off, Seq: r.Seq}
		r.position = t.cursor
		r.frameBytes = uint64(len(line))
		t.batch = append(t.batch, r)
		if !t.mailbox && r.Kind == "seal" {
			var seal segmentSeal
			if json.Unmarshal(r.Value, &seal) == nil && seal.Next == t.seg+1 && seal.Next <= maxSegment {
				t.seal = seal.Next
			}
		}
	}
	return nil
}

func (t *stateTail) damaged(reason string) error {
	t.damage = fmt.Errorf("%w: %s at byte %d: %s", errLogDamage, segmentPath(t.dir, max(t.seg, 1)), t.off, reason)
	return t.damage
}

func (t *stateTail) successor() (uint64, error) {
	if t.seal != 0 {
		return t.seal, nil
	}
	if t.seg < maxSegment {
		if _, err := os.Stat(segmentPath(t.dir, t.seg+1)); err == nil {
			return t.seg + 1, nil
		} else if !os.IsNotExist(err) {
			return 0, err
		}
	}
	if time.Now().Before(t.nextScan) {
		return 0, nil
	}
	t.nextScan = time.Now().Add(time.Second)
	segments, err := t.segments(t.dir)
	if err != nil {
		return 0, err
	}
	for _, seg := range segments {
		if seg > t.seg {
			if t.mailbox && seg != t.seg+1 {
				return 0, t.damaged("missing successor segment")
			}
			return seg, nil
		}
	}
	return 0, nil
}

func (t *stateTail) openNext() (bool, error) {
	seg := max(t.seg, 1)
	ok, err := t.openFile(segmentPath(t.dir, seg))
	if ok || err != nil {
		t.seg = seg
		return ok, err
	}
	if t.mailbox && t.off != 0 {
		return false, t.damaged("missing checkpoint segment")
	}
	now := time.Now()
	if now.Before(t.nextScan) {
		return false, nil
	}
	t.nextScan = now.Add(time.Second)
	segments, err := t.segments(t.dir)
	if err != nil {
		return false, err
	}
	for _, seg := range segments {
		if seg <= t.seg {
			continue
		}
		if t.mailbox && seg != max(t.seg, 1) {
			return false, t.damaged("missing segment")
		}
		t.seg, t.off = seg, 0
		ok, err := t.openFile(segmentPath(t.dir, seg))
		if ok || err != nil {
			return ok, err
		}
	}
	return false, nil
}

func (t *stateTail) openFile(path string) (bool, error) {
	f, err := openJobFile(path, jobFileRead)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if t.mailbox && t.off != 0 {
		info, err := f.Stat()
		if err != nil || uint64(info.Size()) < t.off {
			closeBestEffort(f)
			if err != nil {
				return false, err
			}
			return false, t.damaged("checkpoint past end of segment")
		}
	}
	t.file, t.frames = f, readFrames(f, t.off)
	// A gap after a newly opened segment needs its own immediate probe.
	// Throttle only repeated misses at the same segment.
	t.nextScan = time.Time{}
	return true, nil
}

// Close releases the current read handle.
func (t *stateTail) Close() error {
	if t == nil {
		return nil
	}
	t.closed = true
	if t.file == nil {
		return nil
	}
	err := t.file.Close()
	t.file = nil
	return err
}

func openSegmentedTail(dir string, from stateCursor) (*stateTail, error) {
	if from.Seg > maxSegment || from.Off > math.MaxInt64 || (from.Seg == 0 && from != (stateCursor{})) {
		return nil, fmt.Errorf("invalid log cursor %s", from)
	}
	return &stateTail{dir: dir, seg: from.Seg, off: from.Off, cursor: from, visible: from, syncDir: syncJobDir, segments: logSegments}, nil
}

func openMailboxTail(dir string, from stateCursor) (*stateTail, error) {
	tail, err := openSegmentedTail(dir, from)
	if err == nil {
		tail.mailbox = true
	}
	return tail, err
}

func lockMailbox(ctx context.Context, path string) (*os.File, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	f, err := openJobFile(path, jobFileLockExisting)
	if err != nil {
		return nil, err
	}
	for {
		if err = ctx.Err(); err == nil {
			var locked bool
			locked, err = tryLock(f)
			if locked {
				return f, nil
			}
		}
		if err == nil {
			err = pause(ctx, 20*time.Millisecond)
		}
		if err != nil {
			_ = f.Close()
			return nil, err
		}
	}
}

// Keep both mailbox locks outside the renamed job directory. NTFS can
// reject a directory move while any of its descendants have open handles.
func mailboxLockPath(job, mailbox string) string {
	return filepath.Join(filepath.Dir(job), ".mail-locks", filepath.Base(job), mailbox+".lock")
}

func initMailboxLocks(job string) error {
	for _, name := range []string{"inbox", "outbox"} {
		path := mailboxLockPath(job, name)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return err
		}
		if err := writeJobBytes(path, nil); err != nil && !os.IsExist(err) {
			return err
		}
	}
	return syncJobTree(filepath.Dir(mailboxLockPath(job, "inbox")), filepath.Dir(job))
}

func lockJobMailboxes(ctx context.Context, job string) (func(), error) {
	inbox, err := lockMailbox(ctx, mailboxLockPath(job, "inbox"))
	if err != nil {
		return nil, err
	}
	outbox, err := lockMailbox(ctx, mailboxLockPath(job, "outbox"))
	if err != nil {
		closeBestEffort(inbox)
		return nil, err
	}
	return func() {
		closeBestEffort(outbox)
		closeBestEffort(inbox)
	}, nil
}

// appendJobMailbox binds an operation to its initially observed directory and
// RunID. Holding the mailbox lock serializes it with terminal publication.
func appendJobMailbox(ctx context.Context, job, mailbox string, expected *jobIdentity, r stateRecord, prepare func(*stateRecord, stateRecord, status) error) (stateRecord, error) {
	id, _, _, err := captureJobIdentity(job)
	if err != nil {
		return stateRecord{}, err
	}
	if expected != nil {
		id = *expected
	}
	lockPath := mailboxLockPath(job, mailbox)
	lock, err := lockMailbox(ctx, lockPath)
	if err != nil {
		return stateRecord{}, err
	}
	defer closeBestEffort(lock)
	if err = checkJobHandle(lock, lockPath); err != nil {
		return stateRecord{}, err
	}
	s, _, err := loadJobStatus(job)
	if err != nil {
		return stateRecord{}, err
	}
	if err = id.check(job, s); err != nil {
		return stateRecord{}, err
	}
	return appendSegmented(filepath.Join(job, mailbox), r, func(r *stateRecord, previous stateRecord) error {
		if s.final && previous.Seq == 0 {
			if r.Kind == "close" {
				return errAlreadyClosed
			}
			return fmt.Errorf("job is %s", s.State)
		}
		if prepare != nil {
			return prepare(r, previous, s)
		}
		return nil
	})
}

// Append while holding the corresponding mailbox lock. An immutable hint
// reserves the candidate position before any frame is written. A hint only
// denotes a published operation if that position contains its verified frame.
// Sync the segments used by a new consumer checkpoint. Keeping this
// on the consumer side leaves the mailbox writer's critical section short.
func syncMailboxPrefix(dir string, from, through stateCursor) error {
	if through == from {
		return nil
	}
	for seg := max(from.Seg, 1); seg <= through.Seg; seg++ {
		// The subsequent durable state commit flushes the volume's cache.
		if err := syncExistingJobFile(segmentPath(dir, seg), (*os.File).Sync); err != nil {
			return err
		}
	}
	if through.Seg != from.Seg {
		return syncJobDir(dir)
	}
	return nil
}

func appendSegmented(dir string, r stateRecord, prepare func(*stateRecord, stateRecord) error) (stateRecord, error) {
	if r.Kind == "" || r.Kind == "seal" || r.ID == "" {
		return stateRecord{}, fmt.Errorf("invalid mailbox record kind or ID %q", r.Kind)
	}
	if err := ensureMailboxIndex(dir); err != nil {
		return stateRecord{}, err
	}
	hints := filepath.Join(dir, ".ids", snapSum([]byte(r.ID)))
	previous, err := findMailboxID(dir, hints, r.ID)
	if err != nil {
		return stateRecord{}, err
	}
	if previous.Seq != 0 {
		if prepare != nil {
			if prepareErr := prepare(&r, previous); prepareErr != nil {
				return stateRecord{}, prepareErr
			}
		}
		if previous.Kind != r.Kind || !bytes.Equal(previous.Value, r.Value) {
			return stateRecord{}, fmt.Errorf("log ID %q already has a different record", r.ID)
		}
		return previous, nil
	}
	segments, err := logSegments(dir)
	if err != nil {
		return stateRecord{}, err
	}
	var cursor stateCursor
	seg, mode := uint64(1), jobFileNew
	if len(segments) > 0 {
		seg = segments[len(segments)-1]
		for i := len(segments) - 1; i >= 0 && cursor.Seq == 0; i-- {
			_, cursor, err = latestFrame(segmentPath(dir, segments[i]), segments[i], "")
			if err != nil {
				return stateRecord{}, err
			}
		}
		if cursor.Seg == seg && cursor.Off < defaultRotateBytes {
			info, statErr := os.Stat(segmentPath(dir, seg))
			if statErr != nil {
				return stateRecord{}, statErr
			}
			if uint64(info.Size()) == cursor.Off {
				mode = jobFileAppend
			}
		}
		if mode == jobFileNew {
			seg++
			cursor.Off = 0
		}
	}
	r.Seq, r.Time = cursor.Seq+1, time.Now().UTC()
	if prepare != nil {
		if err = prepare(&r, previous); err != nil {
			return stateRecord{}, err
		}
	}
	line, err := encodeFrame(r)
	if err != nil {
		return stateRecord{}, err
	}
	if err = reserveMailboxPosition(dir, r.ID, stateCursor{Seg: seg, Off: cursor.Off, Seq: r.Seq}); err != nil {
		return stateRecord{}, err
	}
	var f *os.File
	if mode == jobFileNew {
		f, err = createSegment(dir, seg)
	} else {
		f, err = openJobFile(segmentPath(dir, seg), mode)
	}
	if err != nil {
		return stateRecord{}, err
	}
	if err = errors.Join(writeFrame(f.Write, line), f.Close()); err != nil {
		return stateRecord{}, err
	}
	return r, nil
}

func reserveMailboxPosition(dir, id string, at stateCursor) error {
	hints := filepath.Join(dir, ".ids", snapSum([]byte(id)))
	if err := os.MkdirAll(hints, 0700); err != nil {
		return err
	}
	hint := filepath.Join(hints, fmt.Sprintf("%d-%d-%d", at.Seg, at.Off, at.Seq))
	if err := writeJobBytes(hint, nil); err != nil && !os.IsExist(err) {
		return err
	}
	// A persisted log frame can never lack its reservation after power loss.
	return errors.Join(syncJobDir(hints), syncJobDir(filepath.Dir(hints)))
}

// A missing index can be rebuilt under the mailbox lock from the verified
// stream. The durable marker keeps this scan off the append path thereafter.
func ensureMailboxIndex(dir string) error {
	index := filepath.Join(dir, ".ids")
	marker := filepath.Join(index, ".ready")
	if _, err := os.Stat(marker); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := validateSegments(dir, true); err != nil {
		return err
	}
	tail, err := openMailboxTail(dir, stateCursor{})
	if err != nil {
		return err
	}
	defer closeBestEffort(tail)
	records, err := tail.Next()
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.ID == "" {
			return fmt.Errorf("mailbox record %d has no ID", record.Seq)
		}
		at := record.position
		at.Off -= record.frameBytes
		if err := reserveMailboxPosition(dir, record.ID, at); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(index, 0700); err != nil {
		return err
	}
	if err := writeJobBytes(marker, nil); err != nil {
		return err
	}
	return syncJobTree(index, filepath.Dir(dir))
}

func findMailboxID(dir, hints, id string) (stateRecord, error) {
	entries, err := os.ReadDir(hints)
	if os.IsNotExist(err) {
		return stateRecord{}, nil
	}
	if err != nil {
		return stateRecord{}, err
	}
	for _, entry := range entries {
		at, err := parseCursor(strings.ReplaceAll(entry.Name(), "-", ":"))
		if err != nil || at.Seg == 0 || at.Seg > maxSegment || at.Off > math.MaxInt64 {
			return stateRecord{}, fmt.Errorf("invalid mailbox reservation %q", entry.Name())
		}
		f, err := openJobFile(segmentPath(dir, at.Seg), jobFileRead)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return stateRecord{}, err
		}
		reader := readFrames(f, at.Off)
		line, err := reader.line()
		closeBestEffort(f)
		if err == io.EOF {
			continue
		}
		if err != nil {
			return stateRecord{}, err
		}
		if frame, ok := decodeFrame(line); ok && frame.Seq == at.Seq && frame.ID == id {
			return frame, nil
		}
	}
	return stateRecord{}, nil
}
