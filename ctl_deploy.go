package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

type releaseSource struct {
	SHA256 map[string]string `toml:"sha256"`
	URL    string            `toml:"url"`
}
type hostFile struct {
	Hosts   endpoints     `toml:"hosts"`
	Release releaseSource `toml:"release"`
}

func printVersion() error {
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	hash, e := fileHash(exe)
	if e == nil {
		_, e = fmt.Println(hash)
	}
	return e
}
func platform(s string) (string, error) {
	// Windows OpenSSH hands the command to cmd.exe, whose quoting can leave stray quotes around the echo.
	s = strings.ToLower(strings.Trim(strings.TrimSpace(s), "\"' \r\n"))
	osName := ""
	arch := ""
	for _, v := range []string{"linux", "windows", "darwin"} {
		if strings.HasPrefix(s, v+" ") {
			osName = v
		}
	}
	for v := range strings.FieldsSeq(s) {
		switch v {
		case "arm64", "aarch64":
			arch = "arm64"
		case "amd64", "x86_64":
			arch = "amd64"
		case "x86", "i686", "386":
			arch = "386"
		}
	}
	if osName == "" || arch == "" {
		return "", fmt.Errorf("unsupported remote platform %q", s)
	}
	return osName + "/" + arch, nil
}
func detectPlatform(ctx context.Context, h endpoint) (string, error) {
	if h.SSH == "" {
		return runtime.GOOS + "/" + runtime.GOARCH, nil
	}
	if h.relay != nil {
		b, e := json.Marshal(h)
		if e != nil {
			return "", e
		}
		r, e := h.relay.call(ctx, 25*time.Second, "platform-probe", string(b))
		if e != nil {
			return "", e
		}
		if r.code != 0 {
			return "", fmt.Errorf("relay platform probe exited %d", r.code)
		}
		return strings.TrimSpace(string(r.data)), nil
	}
	for _, probe := range []string{"uname -s -m", "cmd /d /c echo windows %PROCESSOR_ARCHITECTURE%"} {
		b, e := h.rawSSH(ctx, "ssh", []string{"--", h.SSH, probe}, "", 90*time.Second) // first connect may need the full budget
		if e == nil {
			if p, e := platform(string(b)); e == nil {
				return p, nil
			}
		}
	}
	return "", errors.New("unable to detect remote OS and CPU via SSH")
}
func configRelease(ctx context.Context, name, plat string) (releaseSource, error) {
	home, e := os.UserHomeDir()
	if e != nil {
		return releaseSource{}, e
	}
	path := filepath.Join(home, ".config", "cx", "hosts.toml")
	var cfg hostFile
	if _, e = os.Stat(path); os.IsNotExist(e) && name == "local" {
		return cfg.Release, nil
	}
	lock, e := lockUntil(ctx, path+".lock")
	if e != nil {
		return cfg.Release, e
	}
	defer closeBestEffort(lock)
	if _, e = toml.DecodeFile(path, &cfg); e != nil {
		return cfg.Release, e
	}
	h, ok := cfg.Hosts[name]
	if !ok && name != "local" {
		return cfg.Release, errors.New("host removed during setup")
	}
	if ok && plat != h.Platform {
		h.Platform = plat
		cfg.Hosts[name] = h
		var b strings.Builder
		if e = toml.NewEncoder(&b).Encode(cfg); e != nil {
			return cfg.Release, e
		}
		e = atomicWrite(path, []byte(b.String()))
	}
	return cfg.Release, e
}
func cachePath(version, plat string) (string, error) {
	if len(version) != 64 {
		return "", errors.New("invalid version")
	}
	if _, e := hex.DecodeString(version); e != nil {
		return "", e
	}
	if normalized, e := platform(strings.Replace(plat, "/", " ", 1)); e != nil || normalized != plat {
		return "", errors.New("invalid platform")
	}
	home, e := os.UserHomeDir()
	name := "cx"
	if strings.HasPrefix(plat, "windows") {
		name += ".exe"
	}
	return filepath.Join(home, ".cache", "cx", "bin", version, strings.ReplaceAll(plat, "/", "-"), name), e
}
func availableArtifact(version, plat string) (string, error) {
	path, err := cachePath(version, plat)
	if err != nil {
		return "", err
	}
	if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
		return path, nil
	} else if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	if seed := os.Getenv("CX_SETUP_BINARY"); seed != "" {
		return seed, nil
	}
	if plat == runtime.GOOS+"/"+runtime.GOARCH {
		return os.Executable()
	}
	return "", nil
}

func expectedVersion(version, plat string) (string, error) {
	src, err := availableArtifact(version, plat)
	if err != nil {
		return "", err
	}
	if src != "" {
		return fileHash(src)
	}
	// Center builds can provide a verified digest without returning the binary.
	path, err := cachePath(version, plat)
	if err != nil {
		return "", err
	}
	b, err := readSharedFile(path + ".sha256")
	if os.IsNotExist(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	digest := strings.TrimSpace(string(b))
	if !snapHex(digest, 64) {
		return "", fmt.Errorf("invalid platform artifact digest in %s.sha256", path)
	}
	return digest, nil
}

func rememberPlatformVersion(version, plat, digest string) error {
	path, err := cachePath(version, plat)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return atomicWrite(path+".sha256", []byte(digest+"\n"))
}

func cacheArtifact(ctx context.Context, version, plat string, release releaseSource) (string, error) {
	path, e := cachePath(version, plat)
	if e != nil {
		return "", e
	}
	dir := filepath.Dir(path)
	if e = os.MkdirAll(dir, 0700); e != nil {
		return "", e
	}
	lock, e := lockUntil(ctx, path+".lock")
	if e != nil {
		return "", e
	}
	defer closeBestEffort(lock)
	if info, statErr := os.Stat(path); statErr == nil && info.Mode().IsRegular() {
		return path, nil
	}
	tmp, e := os.CreateTemp(dir, "build-*")
	if e != nil {
		return "", e
	}
	defer removeBestEffort(tmp.Name())
	if e = tmp.Close(); e != nil {
		return "", e
	}
	seed, e := availableArtifact(version, plat)
	if e != nil {
		return "", e
	}
	goBin, goErr := exec.LookPath("go")
	var module string
	var moduleErr error
	if seed == "" && goErr == nil {
		module, moduleErr = moduleDir()
	}
	if seed != "" {
		e = copyFile(seed, tmp.Name())
	} else if module != "" {
		cmd := exec.CommandContext(ctx, goBin, "build", "-trimpath", "-o", tmp.Name(), ".")
		cmd.Dir = module
		goos, arch, _ := strings.Cut(plat, "/")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS="+goos, "GOARCH="+arch)
		cmd.Stderr = os.Stderr
		e = cmd.Run()
	} else {
		key := strings.ReplaceAll(plat, "/", "-")
		sum := release.SHA256[key]
		if release.URL == "" || len(sum) != 64 {
			if moduleErr != nil {
				return "", fmt.Errorf("build cx for %s (cache miss at %s): %w; or supply CX_SETUP_BINARY", plat, path, moduleErr)
			}
			return "", fmt.Errorf("no Go compiler or configured release URL and SHA-256 for %s", key)
		}
		url := strings.NewReplacer("{version}", version, "{platform}", key).Replace(release.URL)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return "", err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", err
		}
		defer closeBestEffort(resp.Body)
		if resp.StatusCode != 200 {
			return "", fmt.Errorf("release HTTP %d", resp.StatusCode)
		}
		e = writeStream(tmp.Name(), resp.Body)
		if e == nil {
			actual, err := fileHash(tmp.Name())
			if err != nil || !strings.EqualFold(actual, sum) {
				return "", errors.New("release SHA-256 mismatch")
			}
		}
	}
	if e != nil {
		return "", e
	}
	if e = os.Chmod(tmp.Name(), 0700); e != nil {
		return "", e
	}
	return path, os.Rename(tmp.Name(), path)
}
func writeStream(dst string, r io.Reader) error {
	f, e := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_, e = io.Copy(f, r)
	return errors.Join(e, f.Sync(), f.Close())
}
func copyFile(src, dst string) error {
	f, e := os.Open(src)
	if e != nil {
		return e
	}
	defer closeBestEffort(f)
	return writeStream(dst, f)
}
func cacheNode(ctx context.Context, a []string) error {
	if len(a) < 3 {
		return errors.New("missing cache arguments")
	}
	p, e := cachePath(a[1], a[2])
	if e != nil {
		return e
	}
	switch a[0] {
	case "cache-path":
		sum, err := fileHash(p)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(fields{"path": p, "hash": sum})
	case "cache-install":
		if len(a) == 4 {
			if sum, hashErr := fileHash(p); hashErr == nil && sum == a[3] {
				return nil
			}
			sum, stagedHashErr := fileHash(p + ".part")
			if stagedHashErr != nil || sum != a[3] {
				return errors.New("relay cache hash mismatch")
			}
			return os.Rename(p+".part", p)
		}
	case "relay-setup":
		if len(a) == 4 {
			var h endpoint
			if e = json.Unmarshal([]byte(a[3]), &h); e != nil {
				return e
			}
			return setupArtifact(ctx, h, p)
		}
	}
	return errors.New("invalid cache operation")
}
func deploy(ctx context.Context, hs endpoints, name string) error {
	return retryTransfer(ctx, func() error { return deployOnce(ctx, hs, name, false) })
}
func deployOnce(ctx context.Context, hs endpoints, name string, setup bool) error {
	ctx, stop := context.WithTimeout(ctx, transferLimit)
	defer stop()
	h := hs[name]
	if h.Via != "" {
		if err := deployOnce(ctx, hs, h.Via, setup); err != nil {
			return err
		}
		relay := hs[h.Via]
		h.relay = &relay
	}
	plat := h.Platform

	if setup || plat == "" {
		var e error
		plat, e = detectPlatform(ctx, h)
		if e != nil {
			return e
		}
		h = h.withPlatform(plat)
	}
	platformChanged := hs[name].Platform != plat
	hs[name] = h
	exe, e := os.Executable()
	if e != nil {
		return e
	}
	version, e := fileHash(exe)
	if e != nil {
		return e
	}
	expected, e := expectedVersion(version, plat)
	if e != nil {
		return e
	}
	var got reply
	if h.SSH != "" {
		got, e = h.call(ctx, 10*time.Second, "version")
		if e != nil {
			return e
		}
		if expected == "" {
			expected = version
		}
		if got.code != 0 && got.code != missingNodeBinary {
			return got.output()
		}
		if got.code == 0 && strings.TrimSpace(string(got.data)) == expected {
			if platformChanged {
				if _, e = configRelease(ctx, name, plat); e != nil {
					return e
				}
			}
			if !setup {
				return nil
			}
			r, initErr := h.call(ctx, 10*time.Second, "init-host")
			if initErr != nil {
				return initErr
			}
			return r.output()
		}
	}
	release, e := configRelease(ctx, name, plat)
	if e != nil {
		return e
	}
	src, e := availableArtifact(version, plat)
	if e != nil {
		return e
	}
	if src == "" {
		if _, moduleErr := moduleDir(); moduleErr == nil {
			if handled, deployErr := deployFromCenter(ctx, h, plat); handled || deployErr != nil {
				return deployErr
			}
		}
		src, e = cacheArtifact(ctx, version, plat, release)
		if e != nil {
			return e
		}
	}
	if h.Via == "" {
		if h.SSH != "" && strings.HasPrefix(plat, "linux/") && os.Getenv("CX_SETUP_BINARY") == "" {
			digest, hashErr := fileHash(src)
			if hashErr != nil {
				return hashErr
			}
			if got.code == 0 && strings.TrimSpace(string(got.data)) != digest {
				return deltaInstall(ctx, h, src, digest)
			}
		}
		return setupArtifact(ctx, h, src)
	}
	relay := hs[h.Via]
	r, e := relay.call(ctx, 15*time.Second, "cache-path", version, plat)
	if e != nil {
		return e
	}
	if r.code != 0 {
		return r.output()
	}
	var target struct{ Path, Hash string }
	if e = json.Unmarshal(r.data, &target); e != nil {
		return e
	}
	hash, e := fileHash(src)
	if e != nil {
		return e
	}
	if target.Hash != hash {
		if strings.HasPrefix(relay.Platform, "linux/") && os.Getenv("CX_SETUP_BINARY") == "" {
			seed, err := relay.call(ctx, 30*time.Second, "seed-cache", version, plat)
			if err != nil {
				return err
			}
			if seed.code != 0 {
				return seed.output()
			}
			e = rsyncStage(ctx, relay, src, target.Path+".part")
		} else {
			e = upload(ctx, relay, src, target.Path+".part")
		}
		if e != nil {
			return e
		}
		r, e = relay.call(ctx, 15*time.Second, "cache-install", version, plat, hash)
		if e != nil {
			return e
		}
		if r.code != 0 {
			return r.output()
		}
	}
	h.Via = ""
	b, _ := json.Marshal(h)
	r, e = relay.call(ctx, 2*time.Minute, "relay-setup", version, plat, string(b))
	if e != nil {
		return e
	}
	return r.output()
}

func moduleDir() (string, error) {
	candidates := []string{".", "cxrun"}
	if _, file, _, ok := runtime.Caller(0); ok && filepath.IsAbs(file) {
		candidates = append(candidates, filepath.Dir(file))
	}
	if explicit := os.Getenv("CX_MODULE"); explicit != "" {
		candidates = []string{explicit}
	}
	for _, d := range candidates {
		b, e := os.ReadFile(filepath.Join(d, "go.mod"))
		if e != nil {
			continue
		}
		lines := strings.Split(string(b), "\n")
		if m := strings.TrimSpace(lines[0]); m != "module "+modulePath && m != "module cx" {
			continue
		}
		if _, e = os.Stat(filepath.Join(d, "ctl_deploy.go")); e == nil {
			return filepath.Abs(d)
		}
	}
	return "", errors.New("cx Go module unavailable for cross compilation; set CX_MODULE to the cxrun source directory")
}

// modulePath is the public import path of this module (go.mod).
const modulePath = "github.com/djylb/cx"

// A matching cached platform artifact permits hash checking and submission in
// one node invocation. Setup handles empty caches, bootstrap and upgrades.
func submitCommand(ctx context.Context, hs endpoints, name string, args []string) (reply, error) {
	h := hs[name]
	if h.SSH != "" {
		if h.Platform != "" {
			exe, e := os.Executable()
			if e != nil {
				return reply{}, e
			}
			version, e := fileHash(exe)
			if e != nil {
				return reply{}, e
			}
			hash, e := expectedVersion(version, h.Platform)
			if e != nil {
				return reply{}, e
			}
			if hash != "" {
				r, e := h.call(ctx, 30*time.Second, append([]string{"submit-if-version", hash}, args[1:]...)...)
				if e != nil || r.code != 78 && r.code != missingNodeBinary && r.code != 1 {
					return r, e
				}
			}
		}
		if e := deploy(ctx, hs, name); e != nil {
			return reply{}, e
		}
		h = hs[name]
	}
	return h.call(ctx, 30*time.Second, args...)
}
