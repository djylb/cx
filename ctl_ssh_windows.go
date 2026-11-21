package main

import "os"

// The per-user profile directory inherits the user's Windows ACL.
func privateSSHDir(info os.FileInfo) bool { return info.IsDir() }
