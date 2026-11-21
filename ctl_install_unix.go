//go:build !windows

package main

func publishBinary(from, to string) error { return replaceFile(from, to) }
