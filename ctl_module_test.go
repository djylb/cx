package main

import (
	"os"
	"strings"
	"testing"
)

// The real go.mod must be recognised as this module, or cross compilation for remote hosts silently breaks.
func TestModulePathMatchesGoMod(t *testing.T) {
	b, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if first := strings.TrimSpace(strings.SplitN(string(b), "\n", 2)[0]); first != "module "+modulePath {
		t.Fatalf("go.mod declares %q, want module %s", first, modulePath)
	}
	t.Setenv("CX_MODULE", ".")
	if _, err := moduleDir(); err != nil {
		t.Fatal(err)
	}
}
