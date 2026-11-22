package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
)

func scriptExtension(shell string) (string, error) {
	switch shell {
	case "pwsh", "powershell":
		return ".ps1", nil
	case "cmd":
		return ".cmd", nil
	case "sh":
		return ".sh", nil
	}
	return "", errors.New("shell must be pwsh, powershell, cmd or sh")
}

func submitScript(ctx context.Context, h endpoint, args []string, path, shell string) (reply, error) {
	if shell != "" {
		if _, e := scriptExtension(shell); e != nil {
			return reply{}, e
		}
	}
	f, e := os.Open(path)
	if e != nil {
		return reply{}, e
	}
	defer closeBestEffort(f)
	idle, e := transferSetting("CX_TRANSFER_IDLE", 120*time.Second, false)
	if e != nil {
		return reply{}, e
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Hour)
	defer cancel()
	cmd, e := h.command(ctx, append([]string{"submit-script", shell}, args[1:]...))
	if e != nil {
		return reply{}, e
	}
	rd, wr := io.Pipe()
	defer closeBestEffort(rd)
	go func() {
		z := gzip.NewWriter(wr)
		_, copyErr := io.Copy(z, f)
		if closeErr := wr.CloseWithError(errors.Join(copyErr, z.Close())); closeErr != nil {
			diagnosticf("cx: close script input: %v\n", closeErr)
		}
	}()
	cmd.Stdin = rd
	var out, diag bytes.Buffer
	e = runActive(cmd, cancel, h.connectionBudget(idle), idle, &out, &diag)
	if e != nil {
		var ex *exec.ExitError
		if errors.As(e, &ex) && ex.ExitCode() >= 0 && ex.ExitCode() != 255 {
			diagnosticf("%s", cleanDiag(diag.Bytes()))
			return reply{out.Bytes(), ex.ExitCode()}, nil
		}
		diagnosticf("%v %v\n", e, cleanDiag(diag.Bytes()))
		return reply{}, ctlExit(9)
	}
	return reply{out.Bytes(), 0}, nil
}

func receiveScript(ctx context.Context, args []string) error {
	if len(args) < 4 {
		return errors.New("submit-script SHELL JOB CWD")
	}
	shell := scriptShell(args[1])
	ext, e := scriptExtension(shell)
	if e != nil {
		return e
	}
	root, e := stateRoot()
	if e != nil {
		return e
	}
	lock, e := jobOperationLock(ctx, root, args[2])
	if e != nil {
		return e
	}
	defer closeBestEffort(lock)
	dir, e := jobDir(filepath.Join(root, ".scripts"), args[2])
	if e != nil {
		return e
	}
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	z, e := gzip.NewReader(os.Stdin)
	if e != nil {
		return e
	}
	defer closeBestEffort(z)
	f, e := os.CreateTemp(dir, ".script-*")
	if e != nil {
		return e
	}
	defer removeBestEffort(f.Name())
	hash := sha256.New()
	_, e = io.Copy(io.MultiWriter(f, hash, &receiveProgress{}), z)
	if e = errors.Join(e, f.Close()); e != nil {
		return e
	}
	path := filepath.Join(dir, fmt.Sprintf("%x%s", hash.Sum(nil), ext))
	if e = os.Link(f.Name(), path); e != nil && !os.IsExist(e) {
		return e
	}
	argv := scriptArgv(shell, path)
	submit := append([]string{"submit-command"}, args[2:]...)
	return submitNodeCommand(ctx, root, append(append(submit, "--"), argv...))
}

func scriptShell(shell string) string {
	if shell == "" {
		shell = "sh"
		if runtime.GOOS == "windows" {
			shell = "powershell"
		}
	}
	return shell
}
func scriptArgv(shell, path string) []string {
	argv := []string{shell}
	switch shell {
	case "powershell", "pwsh":
		script := "& " + psQuote(path) + "; if (!$?) {if ($LASTEXITCODE) {exit $LASTEXITCODE}; exit 1}"
		return append(argv, psArgs(script)...)
	case "cmd":
		argv = append(argv, "/d", "/c")
	}
	argv = append(argv, path)
	return argv
}
