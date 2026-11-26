package main

import (
	"context"

	"github.com/djylb/timeshim"
)

type clockKey struct{}

// Runner policy and the I/O it waits for can be driven independently.
type runnerClockKey struct{}

func contextClock(ctx context.Context) timeshim.Clock {
	clock, _ := ctx.Value(clockKey{}).(timeshim.Clock)
	return clock
}

func runnerPolicyClock(ctx context.Context) timeshim.Clock {
	clock, _ := ctx.Value(runnerClockKey{}).(timeshim.Clock)
	return clock
}
