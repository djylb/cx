package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

const hostConfigName = "host.json"
const defaultCacheMaxBytes = 50_000_000_000

// Without min_free_bytes, admission keeps the larger of 20 GB and 5% of the volume free.
// Variables only so the test binary (also used as child cx processes) can run
// independently of the free space on the machine running the tests.
var (
	defaultMinFreeBytes   uint64 = 20_000_000_000
	defaultMinFreeDivisor uint64 = 20
)

func (h *hostConfig) minFreeBytes(total uint64) uint64 {
	if h.MinFreeBytes != nil {
		return *h.MinFreeBytes
	}
	if defaultMinFreeDivisor == 0 {
		return defaultMinFreeBytes
	}
	return max(defaultMinFreeBytes, total/defaultMinFreeDivisor)
}

type hostConfig struct {
	Profiles       map[string]hostProfile `json:"profiles,omitempty"`
	Slots          map[string]int         `json:"slots"`
	MinFreeBytes   *uint64                `json:"min_free_bytes,omitempty"`
	EnvFile        string                 `json:"env_file,omitempty"`
	DefaultProfile string                 `json:"default_profile,omitempty"`
	LoadCmd        string                 `json:"load_cmd"`
	Psmux          string                 `json:"psmux,omitempty"`       // Windows: path of psmux.exe when it is not on PATH
	MuxSocket      string                 `json:"mux_socket,omitempty"`  // tmux/psmux -L socket (e.g. a logged-in psmux server)
	MuxSession     string                 `json:"mux_session,omitempty"` // existing psmux session that hosts jobs as windows
	CacheMaxBytes  uint64                 `json:"cache_max_bytes"`
}

func defaultSlots() int {
	switch runtime.GOOS {
	case "linux":
		return defaultLinuxSlots
	case "windows":
		return defaultWindowsSlots
	default:
		return defaultOtherSlots
	}
}

func readHostConfig(root string) (*hostConfig, error) {
	slots := defaultSlots()
	host := &hostConfig{CacheMaxBytes: defaultCacheMaxBytes, Slots: map[string]int{
		agentSlotClass: slots,
		testSlotClass:  slots,
		"system":       defaultSystemSlots,
	}}
	path := filepath.Join(root, hostConfigName)
	if err := readJSON(path, &host); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if host == nil || host.Slots == nil {
		return nil, fmt.Errorf("%s: expected a host object with a slots object", path)
	}
	if e := validateHostSlots(root, host.Slots); e != nil {
		return nil, fmt.Errorf("%s: %w", path, e)
	}
	return host, nil
}
func validateHostSlots(root string, slots map[string]int) error {
	for class, count := range slots {
		if _, err := jobPath(root, class); err != nil {
			return fmt.Errorf("invalid slot class: %w", err)
		}
		if count < 1 {
			return fmt.Errorf("slots for %q must be positive", class)
		}
	}
	return nil
}

func (h *hostConfig) slotCount(class string) int {
	if class == "" {
		class = agentSlotClass
	}
	if count, ok := h.Slots[class]; ok {
		return count
	}
	return defaultSlots()
}

// psmuxLaunch selects the psmux target for a detached worker.
// With mux_session set, the job opens as a window of that existing session, so
// it runs in whatever logon session hosts it (an interactive desktop), not in the
// session of the caller, which over SSH has no window station for GUI-linked
// programs. Without it each job gets its own detached session.
func psmuxLaunch(root, name string) muxWindow {
	program, socket := psmuxSettings(root)
	target := os.Getenv("CX_MUX_SESSION")
	if target == "" {
		if cfg, err := readHostConfig(root); err == nil {
			target = cfg.MuxSession
		}
	}
	if target == "" {
		target = name
	}
	return muxWindow{Program: program, Socket: socket, Session: target, Name: name}
}

// psmuxSettings uses the operation's resolved state root. Environment
// overrides are read directly so one host configuration cannot mask another.
func psmuxSettings(root string) (program, socket string) {
	program = os.Getenv("CX_PSMUX")
	socket = os.Getenv("CXRUN_TMUX_SOCKET")
	if program == "" || socket == "" {
		if cfg, err := readHostConfig(root); err == nil {
			if program == "" {
				program = cfg.Psmux
			}
			if socket == "" {
				socket = cfg.MuxSocket
			}
		}
	}
	if program == "" {
		program = "psmux"
	}
	if socket == "" {
		socket = "cx"
	}
	return program, socket
}
