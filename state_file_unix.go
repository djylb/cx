//go:build darwin || linux

package main

import "os"

func openShared(path string) (*os.File, error) { return os.Open(path) }

func readStateFile(path string) ([]byte, error) { return os.ReadFile(path) }

func replaceFile(from, to string) error { return os.Rename(from, to) }
