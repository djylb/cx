package main

import "os"

func syncJobFile(f *os.File) error { return f.Sync() }
