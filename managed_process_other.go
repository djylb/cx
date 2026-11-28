//go:build !linux && !darwin

package main

func waitScopedExit(int) bool { return false }
