package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func probe(ctx context.Context, argv ...string) string {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.WaitDelay = 200 * time.Millisecond
	b, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
func stringOrNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func osVersion(ctx context.Context) string {
	switch runtime.GOOS {
	case "darwin":
		return probe(ctx, "sw_vers", "-productVersion")
	case "windows":
		return probe(ctx, "cmd", "/d", "/c", "ver")
	case "linux":
		if b, err := os.ReadFile("/etc/os-release"); err == nil {
			for _, line := range strings.Split(string(b), "\n") {
				if v, ok := strings.CutPrefix(line, "PRETTY_NAME="); ok {
					return strings.Trim(v, "\"")
				}
			}
		}
	}
	return probe(ctx, "uname", "-r")
}
func uintValue(s string, mul uint64) any {
	n, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return nil
	}
	return n * mul
}

// hostFactsTimeout bounds one host facts collection; it exceeds engineLoginTimeout.
const hostFactsTimeout = 12 * time.Second

func emptyFacts() fields {
	f := fields{"os": runtime.GOOS, "arch": runtime.GOARCH, "cpu_cores": runtime.NumCPU()}
	for _, k := range strings.Fields("os_version cpu_model memory_total_bytes memory_available_bytes disk_free_bytes load1 cpu_usage_percent engines version_managers") {
		f[k] = nil
	}
	return f
}
func hostFacts(ctx context.Context, root string) fields {
	ctx, cancel := context.WithTimeout(ctx, hostFactsTimeout)
	defer cancel()
	probes := []func() fields{
		func() fields { return fields{"os_version": stringOrNil(osVersion(ctx))} },
		func() fields { return fields{"engines": engineFacts(ctx, nil, true)} },
		func() fields { return fields{"version_managers": managerDirs()} },
		func() fields { return platformFacts(ctx, root) },
	}
	results := make(chan fields, len(probes))
	for _, probe := range probes {
		go func() { results <- probe() }()
	}
	f := emptyFacts()
	for range probes {
		select {
		case values := <-results:
			for k, v := range values {
				f[k] = v
			}
		case <-ctx.Done():
			return f
		}
	}
	return f
}
func managerDirs() any {
	if home, err := os.UserHomeDir(); err == nil {
		managers := fields{}
		dirs := []struct{ name, env, base, sub string }{
			{"mise", "MISE_DATA_DIR", ".local/share/mise", "shims"}, {"asdf", "ASDF_DATA_DIR", ".asdf", "shims"}, {"rustup", "CARGO_HOME", ".cargo", "bin"}, {"pyenv", "PYENV_ROOT", ".pyenv", "shims"}, {"rbenv", "RBENV_ROOT", ".rbenv", "shims"},
		}
		for _, dir := range dirs {
			base := os.Getenv(dir.env)
			if base == "" {
				base = filepath.Join(home, filepath.FromSlash(dir.base))
			}
			name := dir.name
			fi, err := os.Stat(filepath.Join(base, dir.sub))

			switch {
			case err == nil:
				managers[name] = fi.IsDir()
			case os.IsNotExist(err):
				managers[name] = false
			default:
				managers[name] = nil
			}
		}
		return managers
	}
	return nil
}
func diskPath(root string) string {
	// Use an existing ancestor when the requested state root has not been created yet.
	disk, err := filepath.Abs(root)
	if err != nil {
		disk = root
	}
	for {
		_, err := os.Stat(disk)
		if !os.IsNotExist(err) || filepath.Dir(disk) == disk {
			break
		}
		disk = filepath.Dir(disk)
	}
	return disk
}
func platformFacts(ctx context.Context, root string) fields {
	f := fields{}
	if free, _, err := diskSpace(diskPath(root)); err == nil {
		f["disk_free_bytes"] = free
	}
	if runtime.GOOS == "windows" {
		script := `$ErrorActionPreference='Stop';$c=Get-CimInstance Win32_Processor | Select-Object -First 1;$s=Get-CimInstance Win32_OperatingSystem;@{cpu_model=$c.Name;cpu_usage_percent=$c.LoadPercentage;memory_total_bytes=($s.TotalVisibleMemorySize*1024);memory_available_bytes=($s.FreePhysicalMemory*1024)}|ConvertTo-Json -Compress`
		sub, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		c := exec.CommandContext(sub, "powershell", psArgs(script)...)
		c.WaitDelay = 200 * time.Millisecond
		if b, err := c.Output(); err == nil {
			var values fields
			if json.Unmarshal(b, &values) == nil {
				for k, v := range values {
					f[k] = v
				}
			}
		}
		return f
	}
	var load string
	switch runtime.GOOS {
	case "linux":
		b, _ := os.ReadFile("/proc/cpuinfo")
		for _, line := range strings.Split(string(b), "\n") {
			k, v, _ := strings.Cut(line, ":")
			if strings.TrimSpace(k) == "model name" {
				f["cpu_model"] = strings.TrimSpace(v)
				break
			}
		}
		b, _ = os.ReadFile("/proc/meminfo")
		for _, line := range strings.Split(string(b), "\n") {
			c := strings.Fields(line)
			if len(c) != 3 || c[2] != "kB" {
				continue
			}
			switch c[0] {
			case "MemTotal:":
				f["memory_total_bytes"] = uintValue(c[1], 1024)
			case "MemAvailable:":
				f["memory_available_bytes"] = uintValue(c[1], 1024)
			}
		}
		b, _ = os.ReadFile("/proc/loadavg")
		load = string(b)
	case "darwin":
		f["cpu_model"] = stringOrNil(probe(ctx, "sysctl", "-n", "machdep.cpu.brand_string"))
		f["memory_total_bytes"] = uintValue(probe(ctx, "sysctl", "-n", "hw.memsize"), 1)
		f["memory_available_bytes"] = darwinAvailable(probe(ctx, "vm_stat"))
		load = strings.Trim(probe(ctx, "sysctl", "-n", "vm.loadavg"), "{ }")
	}
	if values := strings.Fields(load); len(values) > 0 {
		if n, err := strconv.ParseFloat(values[0], 64); err == nil {
			f["load1"] = n
		}
	}
	return f
}

// Estimate immediately reclaimable Darwin memory using free and inactive pages.
func darwinAvailable(stats string) any {
	var pageSize uint64
	lines := strings.Split(stats, "\n")
	if _, err := fmt.Sscanf(lines[0], "Mach Virtual Memory Statistics: (page size of %d bytes)", &pageSize); err != nil {
		return nil
	}
	var pages uint64
	seen := 0
	for _, line := range lines[1:] {
		name, value, _ := strings.Cut(line, ":")
		if name != "Pages free" && name != "Pages inactive" {
			continue
		}
		n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(value), "."), 10, 64)
		if err != nil {
			return nil
		}
		pages += n
		seen++
	}
	if seen != 2 {
		return nil
	}
	return pages * pageSize
}
func fingerprint(ctx context.Context, dir string) (string, error) {
	fi, err := os.Stat(dir)
	if err != nil {
		return "", err
	}
	if !fi.IsDir() {
		return "", errors.New("expected directory")
	}
	h := sha256.New()
	enc := json.NewEncoder(h)
	record := func(key string, value any) { _ = enc.Encode([]any{key, value}) }
	record("os", []string{runtime.GOOS, runtime.GOARCH, osVersion(ctx)})
	for _, e := range engines {
		record(e.name, probe(ctx, append([]string{e.binary}, e.versionArgs...)...))
	}
	for _, name := range strings.Fields(".mise.toml mise.toml .tool-versions go.mod rust-toolchain.toml rust-toolchain .python-version .nvmrc .node-version package.json") {
		path := filepath.Join(dir, name)
		f, err := os.Open(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		if name == "package.json" {
			var p map[string]json.RawMessage
			err = json.NewDecoder(f).Decode(&p)
			_ = f.Close()
			if err != nil {
				return "", err
			}
			if raw, ok := p["engines"]; ok {
				var value any
				if err = json.Unmarshal(raw, &value); err != nil {
					return "", err
				}
				record(name, value)
			}
			continue
		}
		digest := sha256.New()
		_, err = io.Copy(digest, f)
		_ = f.Close()
		if err != nil {
			return "", err
		}
		record(name, fmt.Sprintf("%x", digest.Sum(nil)))
	}
	tools := strings.Fields("git mise asdf rustup rustc cargo go python python3 node npm java")
	for _, e := range engines {
		tools = append(tools, e.binary)
	}
	for _, cmd := range tools {
		path, err := exec.LookPath(cmd)
		if err != nil {
			continue
		}
		path, err = filepath.Abs(path)
		if err != nil {
			return "", err
		}
		fi, err := os.Stat(path)
		if err != nil {
			return "", err
		}
		record(cmd, []any{path, fi.ModTime().UnixNano()})
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func hostInfoCLI(ctx context.Context, op string, fs *flag.FlagSet, root *string, args []string) error {
	dir := ""
	if op == "notes" {
		if len(args) == 0 || args[0] != "fingerprint" {
			return errors.New("expected notes fingerprint --dir DIR")
		}
		args = args[1:]
		fs.StringVar(&dir, "dir", "", "project directory")
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || (op == "notes" && dir == "") {
		return errors.New("invalid host information arguments")
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if op == "facts" {
		return json.NewEncoder(os.Stdout).Encode(hostFacts(ctx, *root))
	}
	type result struct {
		v   fields
		err error
	}
	done := make(chan result, 1)
	go func() {

		hash, err := fingerprint(ctx, dir)
		done <- result{fields{"fingerprint": hash}, err}
	}()
	select {
	case <-ctx.Done():

		return ctx.Err()
	case r := <-done:
		if r.err != nil {
			return r.err
		}
		return json.NewEncoder(os.Stdout).Encode(r.v)
	}
}
