//go:build !windows

package main

import "context"

func publishBinary(_ context.Context, from, to string) error { return replaceFile(from, to) }
