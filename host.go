package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

const hostConfigName = "host.json"
const defaultCacheMaxBytes = 50_000_000_000

type hostConfig struct {
	EnvFile        string                 `json:"env_file,omitempty"`
	DefaultProfile string                 `json:"default_profile,omitempty"`
	Profiles       map[string]hostProfile `json:"profiles,omitempty"`
	Slots          map[string]int         `json:"slots"`
	LoadCmd        string                 `json:"load_cmd"`
	CacheMaxBytes  uint64                 `json:"cache_max_bytes"`
	MinFreeBytes   *uint64                `json:"min_free_bytes,omitempty"`
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
		codexSlotClass: slots,
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
		class = codexSlotClass
	}
	if count, ok := h.Slots[class]; ok {
		return count
	}
	return defaultSlots()
}
