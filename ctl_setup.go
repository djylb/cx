package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// Offset is measured in uncompressed bytes. A disconnected member retains its
// prefix. The installer verifies the entire staged object before publication.
func receive(ctx context.Context, args []string) error {
	if len(args) < 2 || len(args) > 3 || args[1] != "--offset" {
		return errors.New("recv PATH --offset [N]")
	}
	path := args[0]
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	lock, err := lockUntil(ctx, path+".lock")
	if err != nil {
		return err
	}
	defer closeBestEffort(lock)
	var size int64
	info, err := os.Stat(path)
	if err == nil {
		size = info.Size()
	} else if !os.IsNotExist(err) {
		return err
	}
	if len(args) == 2 {
		fmt.Println(size)
		return nil
	}
	n, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || n != size {
		return errors.New("receive offset mismatch")
	}
	z, err := gzip.NewReader(os.Stdin)
	if err != nil {
		return err
	}
	defer closeBestEffort(z)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = io.Copy(io.MultiWriter(f, &receiveProgress{}), z)
	return errors.Join(err, f.Sync(), f.Close())
}
func fileHash(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer closeBestEffort(f)
	h := sha256.New()
	_, e = io.Copy(h, f)
	return hex.EncodeToString(h.Sum(nil)), e
}
func installNode(ctx context.Context, root, src, dst, digest string) error {
	if dst == "cx" {
		home, e := os.UserHomeDir()
		if e != nil {
			return e
		}
		dst = filepath.Join(home, ".local", "bin", "cx")
		if runtime.GOOS == "windows" {
			dst += ".exe"
		}
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
		return err
	}
	if hash, _ := fileHash(dst); hash != digest {
		f, e := os.Open(src)
		if e != nil {
			return e
		}
		defer closeBestEffort(f)
		tmp, e := os.CreateTemp(filepath.Dir(dst), ".cx-install-*")
		if e != nil {
			return e
		}
		defer removeBestEffort(tmp.Name())
		h := sha256.New()
		_, e = io.Copy(io.MultiWriter(tmp, h), f)
		if e = errors.Join(e, tmp.Chmod(0700), tmp.Sync(), tmp.Close()); e != nil {
			return e
		}
		if hex.EncodeToString(h.Sum(nil)) != digest {
			return errors.New("binary checksum mismatch")
		}
		if e = publishBinary(ctx, tmp.Name(), dst); e != nil {
			return e
		}
	}
	return hostSettings(ctx, root, nil, true)
}
func upload(ctx context.Context, h endpoint, source, dest string) error {
	for i := 0; i < 4; i++ {
		if i > 0 {
			if err := pause(ctx, time.Second<<(i-1)); err != nil {
				return err
			}
		}
		r, e := h.call(ctx, 10*time.Second, "recv", dest, "--offset")
		if e != nil {
			return e
		}
		if r.code != 0 {
			return ctlExit(r.code)
		}
		n, e := strconv.ParseInt(strings.TrimSpace(string(r.data)), 10, 64)
		if e != nil || n < 0 {
			return errors.New("invalid remote offset")
		}
		f, e := os.Open(source)
		if e != nil {
			return e
		}
		st, e := f.Stat()
		if e != nil {
			_ = f.Close()
			return e
		}
		if n > st.Size() {
			_ = f.Close()
			return errors.New("staged object exceeds source length")
		}
		if n == st.Size() {
			_ = f.Close()
			return nil
		}
		if _, e = f.Seek(n, 0); e != nil {
			_ = f.Close()
			return e
		}
		attempt, stop := context.WithCancel(ctx)
		cmd, e := h.command(attempt, []string{"recv", dest, "--offset", strconv.FormatInt(n, 10)})
		if e != nil {
			stop()
			_ = f.Close()
			return e
		}
		rd, wr := io.Pipe()
		cmd.Stdin = rd
		sent := make(chan error, 1)
		go func() {
			z := gzip.NewWriter(wr)
			_, err := io.Copy(z, f)
			err = errors.Join(err, z.Close())
			_ = wr.CloseWithError(err)
			sent <- err
		}()
		var diag bytes.Buffer
		e = runActive(cmd, stop, h.connectionBudget(45*time.Second), 45*time.Second, io.Discard, &diag)
		_ = rd.Close()
		e = errors.Join(e, <-sent)
		if e != nil {
			diagnosticf("%s", cleanDiag(diag.Bytes()))
		}
		stop()
		_ = f.Close()
		if e == nil {
			return nil
		}
	}
	return ctlExit(9)
}
func setupArtifact(ctx context.Context, h endpoint, source string) error {
	ctx, cancel := context.WithTimeout(ctx, 12*time.Minute)
	defer cancel()
	digest, e := fileHash(source)
	if e != nil {
		return e
	}
	if h.SSH == "" {
		root, rootErr := stateRoot()
		if rootErr != nil {
			return rootErr
		}
		if h.State != "" {
			root = h.State
		}
		return installNode(ctx, root, source, h.CX, digest)
	}
	// Use the installed receiver on upgrades. Bootstrap a private copy using
	// OpenSSH SFTP when no receiver is installed.
	r, e := h.call(ctx, 10*time.Second, "version")
	if e != nil {
		return e
	}
	if r.code == 0 && strings.TrimSpace(string(r.data)) == digest {
		r, e = h.call(ctx, 10*time.Second, "init-host")
		if e != nil {
			return e
		}
		return r.output()
	}
	if r.code != 0 && r.code != missingNodeBinary {
		return r.output()
	}
	target := h.CX
	if r.code != 0 {
		var nonce [12]byte
		if _, e = rand.Read(nonce[:]); e != nil {
			return e
		}
		name := fmt.Sprintf(".cx-bootstrap-%x", nonce)
		win := strings.HasPrefix(h.Platform, "windows")
		if win {
			name += ".exe"
		}
		sftp := func(batch string) error {
			_, uploadErr := h.rawSSH(ctx, "sftp", []string{"-b", "-", "--", h.SSH}, batch, 45*time.Second)
			return uploadErr
		}
		if strings.ContainsAny(source, "\r\n") {
			return errors.New("invalid local artifact path")
		}
		defer func() { _ = sftp("rm " + name + "\n") }()
		batch := "put " + strconv.Quote(source) + " " + name + "\n"
		if !win {
			batch += "chmod 700 " + name + "\n"
		}
		if e = sftp(batch); e != nil {
			return e
		}
		h.CX = "./" + name
		r, e = h.call(ctx, 30*time.Second, "install", name, target, digest)
	} else {
		stage := ".cx-upload-" + digest
		if e = upload(ctx, h, source, stage); e != nil {
			return e
		}
		r, e = h.call(ctx, 30*time.Second, "install", stage, target, digest)
	}
	if e != nil {
		return e
	}
	return r.output()
}
