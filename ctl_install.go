package main

import (
	"errors"
	"fmt"
	"time"
)

// publishWithRollback handles a running Windows image that can be moved
// aside but cannot be replaced in place. move must fail if its target exists.
func publishWithRollback(from, to string, replace, move func(string, string) error, retryAfterMove func(error) bool) error {
	err := replace(from, to)
	if !retryAfterMove(err) {
		return err
	}
	aside := fmt.Sprintf("%s.old.%d", to, time.Now().UnixNano())
	if moveErr := move(to, aside); moveErr != nil {
		return errors.Join(err, fmt.Errorf("move running binary %s aside: %w", to, moveErr))
	}
	if err = replace(from, to); err == nil {
		return nil
	}
	if restoreErr := move(aside, to); restoreErr != nil {
		return errors.Join(err, fmt.Errorf("restore %s from %s: %w", to, aside, restoreErr))
	}
	return err
}
