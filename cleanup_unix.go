//go:build !windows

package main

func cleanupError(err error, _ string) error { return err }
