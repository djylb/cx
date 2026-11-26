package main

import (
	"errors"
	"os"
	"runtime"
	"slices"
	"strings"
)

const sessionEnv = "CX_SESSION"

var toolSessionEnv = [...]string{"CLAUDE_CODE_HOST_SESSION_ID", "CLAUDE_CODE_SESSION_ID", "CODEX_THREAD_ID", "OPENCODE_SESSION_ID"}

func currentSession() string {
	if v := os.Getenv(sessionEnv); v != "" {
		return v
	}
	for _, key := range toolSessionEnv {
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	return ""
}

func isSessionEnv(entry string) bool {
	key, _, _ := strings.Cut(entry, "=")
	if runtime.GOOS == "windows" {
		key = strings.ToUpper(key)
	}
	switch key {
	case sessionEnv, "CLAUDE_CODE_CHILD_SESSION", "CODEX_SESSION_ID":
		return true
	}
	return slices.Contains(toolSessionEnv[:], key)
}

func (o ctlOptions) submissionSession() (string, error) {
	if o.noSession {
		return "", nil
	}
	if o.explicitSession {
		return o.session, nil
	}
	if session := currentSession(); session != "" {
		return session, nil
	}
	return "", errors.New("current session unknown; set CX_SESSION or use --session NAME or --no-session")
}
