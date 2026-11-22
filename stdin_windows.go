package main

import "os"

// interruptibleStdin returns standard input; Windows pipe handles are closed by the watchdog directly.
func interruptibleStdin() *os.File { return os.Stdin }
