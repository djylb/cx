package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type markerOptions struct {
	ID   string
	Turn *string
}
type controlMarker struct {
	ID        string  `json:"cmd_id"`
	Turn      *string `json:"turn_id"`
	published time.Time
	Request   json.RawMessage `json:"request_id,omitempty"`
}

func isMarker(op string) bool {
	return op == "close" || op == "interrupt" || op == "approve" || op == "deny"
}
func targetTurn(s status) string {
	if s.TurnStatus == "inProgress" {
		return s.Turn
	}
	return ""
}

// Wait for the runner to finish handling close before reclaiming the checkout.
func deliverClose(ctx context.Context, dir string, opt markerOptions, args []string) (terminal bool, err error) {
	var s status
	if err = projected(ctx, dir, &s); err != nil || isTerminal(s.State) {
		return isTerminal(s.State), err
	}
	if opt.ID == "" {
		opt.ID, err = newMarkerID()
		if err != nil {
			return false, err
		}
	}
	if err = publishMarker(dir, "close", opt, args); err != nil {
		return false, err
	}
	var c config
	if err = readJSON(filepath.Join(dir, "config.json"), &c); err != nil || c.RunID == "" {
		return false, err
	}
	delivery, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for {
		if err = projected(delivery, dir, &s); err != nil || isTerminal(s.State) {
			return isTerminal(s.State), err
		}
		// An explicit turn target can become stale before it is claimed.
		// A discarded marker has neither an inbox entry nor a receipt.
		pending, err := fileExists(filepath.Join(dir, "inbox", "close"))
		if err != nil {
			return false, err
		}
		if !pending {
			claimed, err := fileExists(filepath.Join(dir, "inbox", ".claimed-close"))
			if err != nil {
				return false, err
			}
			if !claimed {
				accepted, err := fileExists(filepath.Join(dir, "inbox", "done", markerReceipt(opt.ID)))
				if err != nil || !accepted {
					return false, err
				}
			}
		}
		if err = pause(delivery, 50*time.Millisecond); err != nil {
			return false, fmt.Errorf("close completion pending: %w", err)
		}
	}
}
func newMarkerID() (string, error) {
	var b [16]byte
	_, err := rand.Read(b[:])
	return hex.EncodeToString(b[:]), err
}

func publishMarker(dir, op string, opt markerOptions, args []string) error {
	var s status
	if err := readJSON(filepath.Join(dir, "status.json"), &s); err != nil {
		return err
	}
	if s.Engine == "cmd" && op != "close" {
		return errors.New("command only accepts close")
	}
	if opt.ID != "" {
		seen, err := fileExists(filepath.Join(dir, "inbox", "done", markerReceipt(opt.ID)))
		if err != nil || seen {
			return err
		}
	}
	if isTerminal(s.State) {
		if op == "close" {
			return nil
		}
		return fmt.Errorf("job is %s", s.State)
	}
	if opt.ID == "" {
		var err error
		opt.ID, err = newMarkerID()
		if err != nil {
			return err
		}
	}
	if opt.Turn == nil {
		turn := targetTurn(s)
		if v, ok := os.LookupEnv("CXRUN_TURN_ID"); ok {
			turn = v
		}
		opt.Turn = &turn
	}
	m := controlMarker{ID: opt.ID, Turn: opt.Turn}
	if len(args) > 0 {
		if len(args) != 1 || (op != "approve" && op != "deny") || !json.Valid([]byte(args[0])) {
			return errors.New("unexpected control arguments")
		}
		m.Request = json.RawMessage(args[0])
	}
	// Preserve an unconsumed command when two controllers publish the same
	// operation. The consumer only renames files and never needs this lock.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	path := filepath.Join(dir, "inbox", op)
	lock, err := lockUntil(ctx, path+".lock")
	if err != nil {
		return err
	}
	defer lock.Close()
	for {
		var previous controlMarker
		err := readJSON(path, &previous)
		if os.IsNotExist(err) {
			return writeJSON(path, m)
		}
		if err == nil && previous.ID == m.ID {
			return nil
		}
		if err = pause(ctx, 20*time.Millisecond); err != nil {
			return fmt.Errorf("%s mailbox busy: %w", op, err)
		}
	}

}

// Claim by rename before reading so a later publication survives this decision.
func (r *runner) takeMarker(op string) (controlMarker, bool, error) {
	var m controlMarker
	path := r.mail(op)
	claimed := r.mail(".claimed-" + op)
	if ok, err := fileExists(claimed); err != nil {
		return m, false, err
	} else if !ok {
		if err = os.Rename(path, claimed); os.IsNotExist(err) {
			return m, false, nil
		} else if err != nil {
			return m, false, err
		}
	}
	fi, err := os.Lstat(claimed)
	if err != nil {
		return m, false, err
	}
	if !fi.Mode().IsRegular() {
		return m, false, fmt.Errorf("invalid control file: %s", op)
	}
	m.published = fi.ModTime()
	b, err := os.ReadFile(claimed)
	if err != nil {
		return m, false, err
	}
	valid := json.Unmarshal(b, &m) == nil && m.ID != "" && m.Turn != nil && *m.Turn == targetTurn(r.s)
	dest := r.mail(filepath.Join("done", markerReceipt(m.ID)))
	if seen, e := fileExists(dest); e != nil {
		return m, false, e
	} else if seen {
		valid = false
	}
	if !valid {
		if err = r.localEvent("control", "discarded stale, invalid or duplicate "+op); err != nil {
			return m, false, err
		}
		return m, false, os.Remove(claimed)
	}
	return m, true, os.Rename(claimed, dest)
}
func markerReceipt(id string) string { return fmt.Sprintf("control-%x", sha256.Sum256([]byte(id))) }
