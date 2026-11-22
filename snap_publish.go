package main

import (
	"archive/tar"
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
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

func snapManifest(store, id string) (Manifest, []byte, error) {
	var m Manifest
	if !snapHex(id, 64) {
		return m, nil, errors.New("invalid snapshot ID")
	}
	b, e := os.ReadFile(filepath.Join(store, "manifests", id+".json"))
	if e != nil {
		return m, nil, e
	}
	if snapSum(b) != id {
		return m, nil, errors.New("manifest checksum mismatch")
	}
	if e = json.Unmarshal(b, &m); e != nil {
		return m, nil, e
	}
	return m, b, m.validate()
}
func snapGroups(m Manifest) map[string][]string {
	groups := map[string][]string{}
	for _, r := range m.Repositories {
		for _, oid := range []string{r.OID, r.Base} {
			if oid != "" && !slices.Contains(groups[r.key()], oid) {
				groups[r.key()] = append(groups[r.key()], oid)
			}
		}
	}
	return groups
}

// Declare owners of trees excluded from sibling snapshots in the bundle header.
func snapPrerequisites(p string, oids []string) error {
	in, e := os.Open(p)
	if e != nil {
		return e
	}
	defer closeBestEffort(in)
	rd := bufio.NewReader(in)
	out, e := os.Create(p + ".new")
	if e != nil {
		return e
	}
	defer closeBestEffort(out)
	line, e := rd.ReadString('\n')
	seen := map[string]bool{}
	for e == nil {
		if _, e = io.WriteString(out, line); e != nil {
			return e
		}
		line, e = rd.ReadString('\n')
		if strings.HasPrefix(line, "-") {
			parts := strings.Fields(line)
			if len(parts) > 0 {
				seen[parts[0][1:]] = true
			}
		} else if !strings.HasPrefix(line, "@") {
			break
		}
	}
	if e != nil {
		return e
	}
	for _, oid := range oids {
		if !seen[oid] {
			if _, e = fmt.Fprintf(out, "-%s published snapshot\n", oid); e != nil {
				return e
			}
		}
	}
	if _, e = io.WriteString(out, line); e != nil {
		return e
	}
	_, e = io.Copy(out, rd)
	if e = errors.Join(e, in.Close(), out.Close()); e != nil {
		return e
	}
	return os.Rename(p+".new", p)
}
func snapArchive(ctx context.Context, store, id, receipts, dst string, full bool) error {
	m, raw, e := snapManifest(store, id)
	if e != nil {
		return e
	}
	tmp, e := os.MkdirTemp(filepath.Dir(dst), ".build-")
	if e != nil {
		return e
	}
	defer removeTreeBestEffort(tmp)
	groups := snapGroups(m)
	seen := map[string]bool{}
	if e = os.WriteFile(filepath.Join(tmp, "manifest.json"), raw, 0600); e != nil {
		return e
	}
	members := []string{"manifest.json"}
	for _, r := range m.Repositories {
		k := r.key()
		if seen[k] {
			continue
		}
		seen[k] = true
		oids := groups[k]
		var old []string
		if !full {
			if prior, readErr := os.ReadFile(filepath.Join(receipts, k)); readErr == nil {
				if prev, _, manifestErr := snapManifest(store, string(prior)); manifestErr == nil {
					old = snapGroups(prev)[k]
				}
			}
		}
		if len(old) > 0 {
			all := true
			for _, oid := range oids {
				all = all && slices.Contains(old, oid)
			}
			if all {
				continue
			}
		}
		name := k + ".bundle"
		g := snapGit{ctx: ctx, dir: r.bare(store)}
		p := filepath.Join(tmp, name)
		args := []string{"bundle", "create", p}
		for _, oid := range oids {
			args = append(args, "refs/snapshots/"+oid)
		}
		if len(old) > 0 {
			args = append(args, "--not")
			for _, oid := range old {
				args = append(args, oid, oid+"^{tree}")
			}
		}
		if _, e = g.get(args...); e != nil {
			return e
		}
		if len(old) > 0 {
			if e = snapPrerequisites(p, old); e != nil {
				return e
			}
		}
		members = append(members, name)
	}
	f, e := os.Create(filepath.Join(tmp, "packet"))
	if e != nil {
		return e
	}
	defer closeBestEffort(f)
	w := tar.NewWriter(f)
	sort.Strings(members)
	for _, name := range members {
		p := filepath.Join(tmp, name)
		st, archiveErr := os.Stat(p)
		if archiveErr != nil {
			return archiveErr
		}
		if archiveErr = w.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: st.Size()}); archiveErr != nil {
			return archiveErr
		}
		input, archiveErr := os.Open(p)
		if archiveErr != nil {
			return archiveErr
		}
		_, archiveErr = io.Copy(w, input)
		if archiveErr = errors.Join(archiveErr, input.Close()); archiveErr != nil {
			return archiveErr
		}
	}
	if e = errors.Join(w.Close(), f.Close()); e != nil {
		return e
	}
	return os.Rename(f.Name(), dst)
}
func snapImport(ctx context.Context, store, id, packet string) error {
	tmp, e := os.MkdirTemp(store, ".import-")
	if e != nil {
		return e
	}
	defer removeTreeBestEffort(tmp)
	f, e := os.Open(packet)
	if e != nil {
		return e
	}
	defer closeBestEffort(f)
	rd := tar.NewReader(f)
	seen := map[string]bool{}
	for {
		h, extractErr := rd.Next()
		if extractErr == io.EOF {
			break
		}
		if extractErr != nil {
			return extractErr
		}
		name := h.Name
		key, ext, _ := strings.Cut(name, ".")
		if seen[name] || h.Typeflag != tar.TypeReg || name != "manifest.json" && (!snapHex(key, 64) || ext != "bundle") {
			return errors.New("invalid snapshot archive member")
		}
		seen[name] = true
		if extractErr = writeStream(filepath.Join(tmp, name), rd); extractErr != nil {
			return extractErr
		}
	}
	raw, e := os.ReadFile(filepath.Join(tmp, "manifest.json"))
	if e != nil {
		return e
	}
	if snapSum(raw) != id {
		return errors.New("invalid publication manifest")
	}
	var m Manifest
	if e = json.Unmarshal(raw, &m); e != nil {
		return e
	}
	if e = m.validate(); e != nil {
		return e
	}
	groups := snapGroups(m)
	for member := range seen {
		if member == "manifest.json" {
			continue
		}
		if _, ok := groups[strings.TrimSuffix(member, ".bundle")]; !ok {
			return errors.New("bundle outside snapshot manifest")
		}
	}
	for _, r := range m.Repositories {
		oids := groups[r.key()]
		if len(oids) == 0 {
			continue
		}
		delete(groups, r.key())
		if e = snapImportGroup(ctx, store, tmp, r, oids); e != nil {
			return e
		}
	}
	for _, r := range m.Repositories {
		g := snapGit{ctx: ctx, dir: r.bare(store)}
		if e = snapVerifyRepo(g, r); e != nil {
			return e
		}
	}
	return snapImmutable(filepath.Join(store, "manifests", id+".json"), raw)
}
func snapImportGroup(ctx context.Context, store, tmp string, r SnapshotRepo, oids []string) error {
	bare := r.bare(store)
	if e := os.MkdirAll(filepath.Dir(bare), 0700); e != nil {
		return e
	}
	lock, e := lockUntil(ctx, bare+".lock")
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	g := snapGit{ctx: ctx, dir: bare}
	if _, e = os.Stat(bare); os.IsNotExist(e) {
		format := "sha1"
		if len(r.OID) == 64 {
			format = "sha256"
		}
		if _, e = (snapGit{ctx: ctx, dir: tmp}).get("init", "-q", "--bare", "--object-format="+format, bare); e != nil {
			return e
		}
	}
	if len(r.Shallow) > 0 {
		if e = snapImmutable(filepath.Join(bare, "shallow"), []byte(strings.Join(r.Shallow, "\n")+"\n")); e != nil {
			return e
		}
	}
	p := filepath.Join(tmp, r.key()+".bundle")
	if _, e = os.Stat(p); e == nil {
		if _, e = g.get("bundle", "unbundle", p); e != nil {
			return e
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	if _, e = g.get(append([]string{"fsck", "--connectivity-only", "--no-dangling", "--no-reflogs"}, oids...)...); e != nil {
		return e
	}
	var refs strings.Builder
	for _, oid := range oids {
		kind, typeErr := g.get("cat-file", "-t", oid)
		if typeErr != nil {
			return typeErr
		}
		if kind != "commit" {
			return errors.New("invalid snapshot commit")
		}
		fmt.Fprintf(&refs, "update refs/snapshots/%s %s\n", oid, oid)
	}
	_, e = g.input([]byte(refs.String()), "update-ref", "--stdin")
	return e
}

// ReceiveSnapshot reads a snapshot packet. Its first stdout line is the durable
// uncompressed offset, or done on a cache hit.
func ReceiveSnapshot(ctx context.Context, args []string) error {
	store, e := snapStore()
	if e != nil {
		return e
	}
	idle, e := snapIdle()
	if len(args) == 4 {
		idle, e = time.ParseDuration(args[3])
		args = args[:3]
		if e == nil && idle <= 0 {
			e = errors.New("invalid snapshot idle timeout")
		}
	}
	if e != nil {
		return e
	}
	// This node invocation owns stdin. Closing it unblocks a silent SSH pipe.
	stop := context.AfterFunc(ctx, func() { _ = os.Stdin.Close() })
	defer stop()
	watchdog := time.AfterFunc(idle, func() { _ = os.Stdin.Close() })
	defer watchdog.Stop()
	return snapReceiveAt(ctx, store, args, snapActiveReader{os.Stdin, watchdog, idle}, os.Stdout)
}
func snapReceiveAt(ctx context.Context, store string, args []string, input io.Reader, reply io.Writer) error {
	if len(args) != 3 || !snapHex(args[0], 64) || !snapHex(args[1], 64) {
		return errors.New("invalid snapshot offer")
	}
	size, e := strconv.ParseInt(args[2], 10, 64)
	if e != nil || size < 0 {
		return errors.New("invalid publication size")
	}
	id := args[0]
	dir := filepath.Join(store, "incoming")
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	lock, e := lockUntil(ctx, filepath.Join(dir, id+".lock"))
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	if _, _, e = snapManifest(store, id); e == nil {
		_, e = fmt.Fprintln(reply, "done")
		return e
	} else if !os.IsNotExist(e) {
		return e
	}
	p := filepath.Join(dir, args[1]+".part")
	if e = receivePacket(p, args[1], size, input, reply); e != nil {
		return e
	}
	process, stop := context.WithTimeout(ctx, 5*time.Minute)
	defer stop()
	if e = snapImport(process, store, id, p); e != nil {
		fmt.Fprintln(os.Stderr, e)
		return ctlExit(65)
	}
	return os.Remove(p)
}

func receivePacket(p, digest string, size int64, input io.Reader, reply io.Writer) error {
	f, e := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer closeBestEffort(f)
	st, e := f.Stat()
	if e != nil {
		return e
	}
	n := st.Size()
	if n > size {
		if e = f.Truncate(0); e != nil {
			return e
		}
		n = 0
	}
	if _, e = fmt.Fprintln(reply, n); e != nil {
		return e
	}
	if n < size {
		z, streamErr := gzip.NewReader(input)
		if streamErr != nil {
			return streamErr
		}
		defer closeBestEffort(z)
		last := time.Time{}
		meter := &snapProgress{bytes: n, report: func(sizeNow int64) {
			if sizeNow == size || time.Since(last) >= time.Second {
				_, _ = fmt.Fprintln(reply, "bytes", sizeNow)
				last = time.Now()
			}
		}}
		w := io.MultiWriter(f, &receiveProgress{}, meter)
		written, streamErr := io.Copy(w, io.LimitReader(z, size-n+1))
		if streamErr != nil {
			return streamErr
		}
		if written != size-n {
			return fmt.Errorf("incomplete snapshot payload: got %d, expected %d", written, size-n)
		}
	}
	if e = errors.Join(f.Sync(), f.Close()); e != nil {
		return e
	}
	hash, e := fileHash(p)
	if e != nil {
		return e
	}
	if hash != digest {
		_ = os.Remove(p)
		return errors.New("archive checksum mismatch")
	}
	return nil
}

type snapAck struct {
	ready  chan string
	report func(int64)
	buf    string
	sent   bool
}

func (a *snapAck) Write(b []byte) (int, error) {
	a.buf += string(b)
	for {
		line, rest, ok := strings.Cut(a.buf, "\n")
		if !ok {
			if len(a.buf) > 128 {
				return 0, errors.New("invalid snapshot progress")
			}
			return len(b), nil
		}
		a.buf = rest
		line = strings.TrimSpace(line)
		if !a.sent {
			a.sent = true
			a.ready <- line
		}
		if a.report != nil {
			n, e := strconv.ParseInt(strings.TrimPrefix(line, "bytes "), 10, 64)
			if e == nil {
				a.report(n)
			}
		}
	}
}
func snapSend(ctx context.Context, hub endpoint, args []string, p string, progress func(int64)) error {
	idle, e := snapIdle()
	if e != nil {
		return e
	}
	return sendPacket(ctx, hub, append(append([]string{"snap-receive"}, args...), idle.String()), p, progress, idle)
}
func sendPacket(ctx context.Context, hub endpoint, args []string, p string, progress func(int64), idle time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd, e := hub.command(ctx, args)
	if e != nil {
		return e
	}
	rd, wr, e := os.Pipe()
	if e != nil {
		return e
	}
	defer closeBestEffort(rd)
	defer closeBestEffort(wr)
	cmd.Stdin = rd
	a := &snapAck{ready: make(chan string, 1), report: progress}
	done := make(chan error, 1)
	var diag bytes.Buffer
	go func() {
		err := runActive(cmd, cancel, hub.connectionBudget(idle), idle, a, &diag)
		_ = rd.Close()
		done <- err
	}()
	var line string
	select {
	case line = <-a.ready:
	case e = <-done:
		if e == nil && a.sent {
			return nil
		}
		if e == nil {
			return errors.New("missing transfer acknowledgement")
		}
		return fmt.Errorf("transfer handshake: %w %s", e, cleanDiag(diag.Bytes()))
	}
	if line != "done" {
		var n int64
		n, e = strconv.ParseInt(line, 10, 64)
		if e == nil {
			var f *os.File
			f, e = os.Open(p)
			if e == nil {
				defer closeBestEffort(f)
				var st os.FileInfo
				st, e = f.Stat()
				if e == nil && (n < 0 || n > st.Size()) {
					e = errors.New("invalid receive offset")
				}
				if e == nil && n < st.Size() {
					_, e = f.Seek(n, 0)
					if e == nil {
						z := gzip.NewWriter(wr)
						_, e = io.Copy(z, f)
						e = errors.Join(e, z.Close())
					}
				}

			}
		}
	}
	_ = wr.Close()
	if e != nil {
		cancel()
	}
	runErr := <-done
	if runErr != nil {
		return fmt.Errorf("packet receive: %w: %s", errors.Join(runErr, e), cleanDiag(diag.Bytes()))
	}
	return e
}
func Publish(ctx context.Context, hub endpoint, sid SnapshotID, progress func(int64)) error {
	store, e := snapStore()
	if e != nil {
		return e
	}
	id := string(sid)
	m, _, e := snapManifest(store, id)
	if e != nil {
		return e
	}
	local := hub.SSH == "" && hub.relay == nil
	target := store
	if local {
		if hub.State != "" {
			target = filepath.Join(hub.State, "snapshot")
		}
		if _, _, err := snapManifest(target, id); err == nil {
			if progress != nil {
				progress(0)
			}
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	dest, _ := json.Marshal(hub)
	receipts := filepath.Join(store, "published", snapSum(dest))
	if e = os.MkdirAll(receipts, 0700); e != nil {
		return e
	}
	l, e := transferLock(ctx, filepath.Join(receipts, id+".lock"))
	if e != nil {
		return e
	}
	defer closeBestEffort(l)
	p := filepath.Join(receipts, id+".tar")
	full := false
	return retryTransfer(ctx, func() error {
		for {
			if _, e = os.Stat(p); os.IsNotExist(e) {
				if e = snapArchive(ctx, store, id, receipts, p, full); e != nil {
					if full {
						return e
					}
					full = true
					if e = snapArchive(ctx, store, id, receipts, p, true); e != nil {
						return e
					}
				}
			} else if e != nil {
				return e
			}
			if local {
				if e = os.MkdirAll(target, 0700); e == nil {
					e = snapImport(ctx, target, id, p)
				}

			} else {
				h, err := fileHash(p)
				if err != nil {
					return err
				}
				st, err := os.Stat(p)
				if err != nil {
					return err
				}
				e = snapSend(ctx, hub, []string{id, h, strconv.FormatInt(st.Size(), 10)}, p, progress)
			}
			if e == nil {
				for k := range snapGroups(m) {
					if e = atomicWrite(filepath.Join(receipts, k), []byte(id)); e != nil {
						return e
					}
				}
				return os.Remove(p)
			}
			var ex *exec.ExitError
			if !full && (local || errors.As(e, &ex) && ex.ExitCode() == 65) {
				full = true
				if err := os.Remove(p); err != nil {
					return err
				}
				continue
			}
			return e
		}
	})
}

type snapProgress struct {
	report func(int64)
	bytes  int64
}

func (w *snapProgress) Write(b []byte) (int, error) {
	w.bytes += int64(len(b))
	w.report(w.bytes)
	return len(b), nil
}

func snapVerifyRepo(g snapGit, r SnapshotRepo) error {
	for _, oid := range []string{r.OID, r.Base} {
		if oid == "" {
			continue
		}
		actual, e := g.get("rev-parse", "--verify", "refs/snapshots/"+oid+"^{commit}")
		if e != nil {
			return e
		}
		if actual != oid {
			return errors.New("snapshot ref mismatch")
		}
	}
	if r.Base != "" {
		parents, e := g.get("rev-list", "-1", "--parents", r.OID)
		if e != nil {
			return e
		}
		if parents != r.OID+" "+r.Base {
			return errors.New("invalid snapshot parent")
		}
		if r.History {
			if r.Head == nil || r.Base != *r.Head {
				return errors.New("invalid history base")
			}
		} else {
			parents, e = g.get("rev-list", "-1", "--parents", r.Base)
			if e != nil {
				return e
			}
			if parents != r.Base {
				return errors.New("invalid synthetic base")
			}
		}
	}
	return nil
}

type snapActiveReader struct {
	io.Reader
	timer *time.Timer
	idle  time.Duration
}

func (r snapActiveReader) Read(b []byte) (int, error) {
	n, e := r.Reader.Read(b)
	if n > 0 {
		r.timer.Reset(r.idle)
	}
	return n, e
}
func snapIdle() (time.Duration, error) {
	return transferSetting("CX_PUBLISH_IDLE", 120*time.Second, false)
}
