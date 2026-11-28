package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
)

const (
	commandOutputName   = "output.log"
	commandOutputStream = ".output-stream"
	commandOutputCursor = ".output-cursor"
	outputCursorBytes   = 8
)

// The copier writes the private stream. Only completed, admitted writes can
// advance the public prefix. Sealing closes admission without waiting for a
// write that the process drain has already abandoned.
type commandOutput struct {
	stream, cursor *os.File
	end            atomic.Int64 // Nonnegative while open, ^length once sealed.
}

func openCommandOutputWriter(dir string) (*commandOutput, error) {
	stream, err := openJobFile(filepath.Join(dir, commandOutputStream), jobFileAppend)
	if err != nil {
		return nil, err
	}
	cursor, err := openJobFile(filepath.Join(dir, commandOutputCursor), jobFileAppend)
	if err != nil {
		return nil, errors.Join(err, stream.Close())
	}
	return &commandOutput{stream: stream, cursor: cursor}, nil
}

func (w *commandOutput) length() int64 {
	end := w.end.Load()
	if end < 0 {
		return ^end
	}
	return end
}

func (w *commandOutput) Write(p []byte) (int, error) {
	return w.write(p, w.stream.Write)
}

func (w *commandOutput) write(p []byte, appendBytes func([]byte) (int, error)) (int, error) {
	start := w.end.Load()
	if start < 0 {
		return 0, errOutputTruncated
	}
	n, err := appendBytes(p)
	if n == 0 {
		return n, err
	}
	end := start + int64(n)
	if !w.end.CompareAndSwap(start, end) {
		return n, errors.Join(err, errOutputTruncated)
	}
	var marker [outputCursorBytes]byte
	binary.LittleEndian.PutUint64(marker[:], uint64(end))
	return n, errors.Join(err, writeFrame(w.cursor.Write, marker[:]))
}

func (w *commandOutput) sealLength() int64 {
	for {
		end := w.end.Load()
		if end < 0 {
			return ^end
		}
		if w.end.CompareAndSwap(end, ^end) {
			return end
		}
	}
}

func (w *commandOutput) seal(ctx context.Context, dir string, abandoned bool) (int64, error) {
	length := w.sealLength()
	source := w.stream.Name()
	if abandoned {
		// Closing an os.File can wait for an in-flight write. Those writes
		// may extend the spool, but they can no longer advance its prefix.
		go func() {
			closeBestEffort(w.stream)
			closeBestEffort(w.cursor)
		}()
	} else if err := errors.Join(w.stream.Close(), w.cursor.Close()); err != nil {
		return length, err
	}
	in, err := openJobFile(source, jobFileRead)
	if err != nil {
		return length, err
	}
	defer closeBestEffort(in)
	// In the usual case one inode serves both names. If the copier was
	// abandoned, publish an independent inode even when the sizes match.
	if abandoned {
		f, err := createJobTemp(dir, ".output-")
		if err != nil {
			return length, err
		}
		source = f.Name()
		defer removeBestEffort(source)
		_, err = io.CopyN(f, resultReader{ctx, in}, length)
		if err = errors.Join(err, f.Close()); err != nil {
			return length, err
		}
	}
	if err := syncExistingJobFile(source, syncJobFile); err != nil {
		return length, err
	}
	if err := os.Link(source, filepath.Join(dir, commandOutputName)); err != nil {
		return length, err
	}
	return length, syncJobDir(dir)
}

func openCommandOutput(dir string, identity *jobIdentity) (*os.File, int64, error) {
	final, err := openObservedFile(dir, commandOutputName, identity)
	if err == nil {
		info, statErr := final.Stat()
		if statErr != nil {
			return nil, 0, errors.Join(statErr, final.Close())
		}
		return final, info.Size(), nil
	}
	if !os.IsNotExist(err) {
		return nil, 0, err
	}
	cursor, err := openObservedFile(dir, commandOutputCursor, identity)
	if err != nil {
		return nil, 0, err
	}
	defer closeBestEffort(cursor)
	info, err := cursor.Stat()
	if err != nil {
		return nil, 0, err
	}
	var length int64
	if end := info.Size() / outputCursorBytes * outputCursorBytes; end > 0 {
		var marker [outputCursorBytes]byte
		if _, err = cursor.ReadAt(marker[:], end-outputCursorBytes); err != nil {
			return nil, 0, err
		}
		length = int64(binary.LittleEndian.Uint64(marker[:]))
		if length < 0 {
			return nil, 0, errors.New("invalid command output cursor")
		}
	}
	live, err := openObservedFile(dir, commandOutputStream, identity)
	return live, length, err
}

func streamCommandOutput(out io.Writer, dir string, off int64, s status, identity *jobIdentity) (int64, error) {
	f, length, err := openCommandOutput(dir, identity)
	if err != nil {
		return 0, err
	}
	defer closeBestEffort(f)
	size := max(0, length-off)
	n, err := io.CopyN(out, io.NewSectionReader(f, off, size), size)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	if err == nil && s.final {
		err = errDrained
	}
	return n, err
}
