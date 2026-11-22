package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type hostProfile struct {
	CodexHome       string `json:"codex_home,omitempty"`
	ClaudeConfigDir string `json:"claude_config_dir,omitempty"`
	EnvFile         string `json:"env_file,omitempty"`
}

// Empty is a valid recorded selection. Only a new submission uses the default.
func (h *hostConfig) profile(name string) (hostProfile, error) {
	p, ok := h.Profiles[name]
	if name != "" && !ok {
		return p, fmt.Errorf("unknown profile %q", name)
	}
	if name == "" {
		p = hostProfile{}
	}
	if p.EnvFile == "" {
		p.EnvFile = h.EnvFile
	}
	return p, nil
}

func selectedProfile(root, name string, useDefault bool) (string, hostProfile, error) {
	h, err := readHostConfig(root)
	if err != nil {
		return "", hostProfile{}, err
	}
	if name == "" && useDefault {
		name = h.DefaultProfile
	}
	p, err := h.profile(name)
	return name, p, err
}

func stateRelative(root, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(root, path)
}

func (p hostProfile) accountEnv(root string) []string {
	var env []string
	if p.CodexHome != "" {
		env = append(env, "CODEX_HOME="+stateRelative(root, p.CodexHome))
	}
	if p.ClaudeConfigDir != "" {
		env = append(env, "CLAUDE_CONFIG_DIR="+stateRelative(root, p.ClaudeConfigDir))
	}
	return env
}

func parseEnvFile(data string) (map[string]string, error) {
	env := map[string]string{}
	for i, line := range strings.Split(data, "\n") {
		line = strings.TrimSuffix(line, "\r")
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || !envName.MatchString(key) || strings.ContainsRune(value, 0) || !utf8.ValidString(value) {
			return nil, fmt.Errorf("invalid env_file assignment on line %d", i+1)
		}
		env[key] = value
	}
	return env, nil
}

// The runtime environment is deliberately unexported in config. Only the
// payload gets these values, including when its runner was launched via a mux.
func (r *runner) loadCredentials() error {
	root, err := filepath.Abs(r.root)
	if err != nil {
		return err
	}
	_, p, err := selectedProfile(root, r.s.Profile, false)
	if err != nil {
		return err
	}
	// Non-interactive launches (tmux/psmux, ssh) skip shell rc files, so version-manager shims are missing and
	// an older system toolchain wins. Put existing shim directories first; env_file below may still set PATH.
	if path := shimPath(os.Getenv("PATH")); path != os.Getenv("PATH") {
		r.c.localEnv = append(r.c.localEnv, "PATH="+path)
		// The runner is a per-job process: program lookup (exec.Command) uses this process's PATH.
		_ = os.Setenv("PATH", path)
	}
	if p.EnvFile != "" {
		path := stateRelative(root, p.EnvFile)
		info, err := os.Stat(path)
		if err == nil && !info.Mode().IsRegular() {
			return fmt.Errorf("env_file is not a regular file")
		}
		var data []byte
		if err == nil {
			data, err = os.ReadFile(path)
		}
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("read env_file: %w", err)
		}
		if err == nil {
			env, err := parseEnvFile(string(data))
			if err != nil {
				return err
			}
			keys := make([]string, 0, len(env))
			for key, value := range env {
				keys = append(keys, key)
				r.c.localEnv = append(r.c.localEnv, key+"="+value)
			}
			slices.Sort(keys)
			r.c.secret, err = newSecretFilter(env)
			if err != nil {
				return err
			}
			method, _ := json.Marshal("env_file/loaded")
			params, _ := json.Marshal(fields{"keys": keys})
			f, err := os.OpenFile(filepath.Join(r.dir, "events.jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			err = r.appendEvent(f, map[string]json.RawMessage{"method": method, "params": params})
			closeErr := f.Close()
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
		}
	}
	r.c.localEnv = append(r.c.localEnv, p.accountEnv(root)...)
	return nil
}

func sensitiveKey(key string) bool {
	key = strings.ToUpper(key)
	return strings.HasSuffix(key, "_API_KEY") || strings.Contains(key, "TOKEN") ||
		strings.Contains(key, "SECRET") || strings.Contains(key, "PASSWORD")
}

func warnCredentialKey(key string) {
	fmt.Fprintf(os.Stderr, "cx: %s looks like a credential; put it in the executor env_file instead\n", key)
}

func warnInputCredentials(data []byte) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	seen := map[string]bool{}
	var scan func(json.Token)
	scan = func(token json.Token) {
		switch token {
		case json.Delim('{'):
			for dec.More() {
				k, _ := dec.Token()
				key, _ := k.(string)
				if sensitiveKey(key) && !seen[key] {
					warnCredentialKey(key)
					seen[key] = true
				}
				value, _ := dec.Token()
				scan(value)
			}
			_, _ = dec.Token()
		case json.Delim('['):
			for dec.More() {
				value, _ := dec.Token()
				scan(value)
			}
			_, _ = dec.Token()
		}
	}
	first, _ := dec.Token()
	scan(first)
}

// The filter handles literal values from env_file in cx-captured output.
// Ordering longer values first also handles values that share a prefix.
type secretFilter struct {
	pattern *regexp.Regexp
	values  []string
}

func newSecretFilter(env map[string]string) (*secretFilter, error) {
	values := make([]string, 0, len(env))
	for _, v := range env {
		if v != "" {
			values = append(values, v)
		}
	}
	if len(values) == 0 {
		return nil, nil
	}
	slices.SortFunc(values, func(a, b string) int { return len(b) - len(a) })
	patterns := make([]string, len(values))
	for i, v := range values {
		patterns[i] = regexp.QuoteMeta(v)
	}
	pattern, err := regexp.Compile(strings.Join(patterns, "|"))
	if err != nil {
		// Regexp diagnostics can quote the pattern, which contains credentials.
		return nil, fmt.Errorf("env_file values exceed filtering capacity")
	}
	return &secretFilter{pattern, values}, nil
}

const hiddenValue = "[REDACTED]"

func (f *secretFilter) text(s string) string {
	if f == nil {
		return s
	}
	return f.pattern.ReplaceAllLiteralString(s, hiddenValue)
}

// jsonOutput filters the captured copy of an app-server payload. Dispatch uses
// the original frame. Only fields carrying display text are eligible.
func (f *secretFilter) jsonOutput(raw json.RawMessage) (json.RawMessage, error) {
	if f == nil || len(raw) == 0 {
		return raw, nil
	}
	var data any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&data); err != nil {
		return nil, fmt.Errorf("invalid agent JSON")
	}
	var redact func(any, bool, bool) any
	redact = func(v any, arguments, changePaths bool) any {
		switch x := v.(type) {
		case string:
			if arguments {
				return f.text(x)
			}
		case map[string]any:
			for k, child := range x {
				if strings.HasSuffix(k, "Id") || strings.HasSuffix(k, "ID") || strings.HasSuffix(k, "_id") {
					continue
				}
				switch strings.ToLower(k) {
				case "id", "jsonrpc", "method", "type", "status", "state", "permissions":
					continue
				case "arguments":
					x[k] = redact(child, true, changePaths)
					continue
				case "changes":
					x[k] = redact(child, arguments, x["type"] == "fileChange")
					continue
				case "path":
					if path, ok := child.(string); ok && changePaths {
						x[k] = f.text(path)
						continue
					}
				case "text":
					if output, ok := child.(string); ok {
						x[k] = f.reportText(output)
						continue
					}
				case "delta", "content", "output", "aggregatedoutput", "stdin", "stdout", "stderr",
					"message", "additionaldetails", "explanation", "step", "diff", "command",
					"patch", "reason", "summary", "question", "header", "description", "detail", "label":
					if output, ok := child.(string); ok {
						x[k] = f.text(output)
						continue
					}
				}
				x[k] = redact(child, arguments, changePaths)
			}
		case []any:
			for i, child := range x {
				x[i] = redact(child, arguments, changePaths)
			}
		}
		return v
	}
	return json.Marshal(redact(data, false, false))
}

// First-line report status is consumed by both the controller and other agents.
func (f *secretFilter) reportText(text string) string {
	if f == nil {
		return text
	}
	first, body, multiline := strings.Cut(text, "\n")
	if reportStatus(first) == "" {
		return f.text(text)
	}
	if multiline {
		return first + "\n" + f.text(body)
	}
	return first
}

type filteredOutput struct {
	dst     io.Writer
	filter  *secretFilter
	pending []byte // under mu
	mu      sync.Mutex
	closed  bool // under mu
}

func (w *filteredOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return len(p), nil
	}
	w.pending = append(w.pending, p...)
	// Hold only a trailing partial match so ordinary output remains live.
	limit := len(w.pending)
	for _, value := range w.filter.values {
		for i := max(0, len(w.pending)-len(value)+1); i < limit; {
			start := bytes.IndexByte(w.pending[i:limit], value[0])
			if start < 0 {
				break
			}
			i += start
			if strings.HasPrefix(value, string(w.pending[i:])) {
				limit = i
				break
			}
			i++
		}
	}
	return len(p), w.flush(limit)
}

func (w *filteredOutput) flush(limit int) error {
	if limit == 0 {
		return nil
	}
	end := 0
	var out bytes.Buffer
	for _, match := range w.filter.pattern.FindAllIndex(w.pending, -1) {
		if match[0] >= limit {
			break
		}
		out.Write(w.pending[end:match[0]])
		out.WriteString(hiddenValue)
		end = match[1]
	}
	limit = max(limit, end)
	out.Write(w.pending[end:limit])
	_, err := w.dst.Write(out.Bytes())
	w.pending = append(w.pending[:0], w.pending[limit:]...)
	return err
}

func (f *secretFilter) output(cmd *exec.Cmd, dst *os.File) (io.Writer, func() error) {
	if f == nil {
		return dst, func() error { return nil }
	}
	w := &filteredOutput{dst: dst, filter: f}
	// Descendants can outlive their foreground payload and inherit its pipes.
	cmd.WaitDelay = 200 * time.Millisecond
	return w, func() error {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.closed = true
		return w.flush(len(w.pending))
	}
}

func checkResumeProfile(root, recorded, requested string) error {
	if requested != "" && requested != recorded {
		if _, _, err := selectedProfile(root, requested, false); err != nil {
			return err
		}
		return fmt.Errorf("resume uses the original profile %q", recorded)
	}
	_, _, err := selectedProfile(root, recorded, false)
	return err
}

// shimPath prepends the shim directories of installed version managers (mise, asdf, pyenv, rbenv) that are not
// already on PATH. Which manager exists is a fixed fact (its directory); versions are resolved by the shims.
func shimPath(current string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		return current
	}
	dirs := []struct{ env, base, sub string }{
		{"MISE_DATA_DIR", ".local/share/mise", "shims"}, {"ASDF_DATA_DIR", ".asdf", "shims"},
		{"PYENV_ROOT", ".pyenv", "shims"}, {"RBENV_ROOT", ".rbenv", "shims"},
		// The managers' own executables (e.g. `mise exec ...`): mise installs to ~/.local/bin by default.
		{"", ".local", "bin"}, {"ASDF_DIR", ".asdf", "bin"}, {"PYENV_ROOT", ".pyenv", "bin"}, {"RBENV_ROOT", ".rbenv", "bin"},
	}
	if runtime.GOOS == "windows" {
		if local := os.Getenv("LOCALAPPDATA"); local != "" {
			dirs = append(dirs, struct{ env, base, sub string }{"", filepath.Join(local, "mise"), "shims"})
		}
	}
	have := map[string]bool{}
	for _, p := range filepath.SplitList(current) {
		have[p] = true
	}
	var front []string
	for _, d := range dirs {
		base := ""
		if d.env != "" {
			base = os.Getenv(d.env)
		}
		if base == "" {
			base = d.base
			if !filepath.IsAbs(base) {
				base = filepath.Join(home, filepath.FromSlash(base))
			}
		}
		dir := filepath.Join(base, d.sub)
		if fi, err := os.Stat(dir); err == nil && fi.IsDir() && !have[dir] {
			front = append(front, dir)
			have[dir] = true
		}
	}
	if len(front) == 0 {
		return current
	}
	return strings.Join(append(front, current), string(os.PathListSeparator))
}
