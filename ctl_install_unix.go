//go:build !windows

package main

import (
	"context"
	"os"
)

func publishBinary(_ context.Context, from, to string) error { return os.Rename(from, to) }
