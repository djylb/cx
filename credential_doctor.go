package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

func envFileDoctor(root, path string) fields {
	f := fields{"configured": path != ""}
	if path == "" {
		return f
	}
	fi, err := os.Stat(stateRelative(root, path))
	f["exists"] = err == nil
	switch {
	case os.IsNotExist(err):
		f["warning"] = "env_file missing; runner skips it"
	case err != nil:
		f["warning"] = "env_file could not be inspected"
	case !fi.Mode().IsRegular():
		f["warning"] = "env_file is not a regular file"
	case runtime.GOOS != "windows":
		mode := fi.Mode().Perm()
		f["mode"] = fmt.Sprintf("%04o", mode)
		if mode&^0600 != 0 {
			f["warning"] = "env_file permissions are broader than 0600"
		}
	}
	return f
}

func credentialDoctor(ctx context.Context, root string, h *hostConfig, out fields) {
	if abs, err := filepath.Abs(root); err == nil {
		root = abs
	}
	_, claudeErr := exec.LookPath("claude")
	out["claude_available"] = claudeErr == nil
	out["env_file"] = envFileDoctor(root, h.EnvFile)
	if h.DefaultProfile != "" {
		out["default_profile"] = h.DefaultProfile
		if _, err := h.profile(h.DefaultProfile); err != nil {
			out["default_profile_warning"] = err.Error()
		}
	}
	type probeResult struct {
		name string
		info fields
	}
	results := make(chan probeResult, len(h.Profiles))
	for name := range h.Profiles {
		p, _ := h.profile(name)
		go func() {
			results <- probeResult{name, fields{
				"codex_login_status": loginStatus(ctx, p.accountEnv(root)),
				"claude_available":   claudeErr == nil,
				"env_file":           envFileDoctor(root, p.EnvFile),
			}}
		}()
	}
	profiles := fields{}
	for range h.Profiles {
		result := <-results
		profiles[result.name] = result.info
	}
	out["profiles"] = profiles
}
