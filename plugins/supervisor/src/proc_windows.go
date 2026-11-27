//go:build windows

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

const (
	createNoWindow                 = 0x08000000
	processQueryLimitedInformation = 0x1000
	stillActive                    = 259
)

func configureChildProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | createNoWindow,
	}
	cmd.Cancel = func() error {
		killProcessGroup(cmd.Process.Pid)
		return cmd.Process.Kill()
	}
}

func taskkill(args ...string) {
	bin := "taskkill.exe"
	if root := os.Getenv("SystemRoot"); root != "" {
		bin = filepath.Join(root, "System32", "taskkill.exe")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: createNoWindow}
	_ = cmd.Run()
}

func terminateProcessGroup(pid int) {
	if pid <= 0 {
		return
	}
	taskkill("/T", "/PID", strconv.Itoa(pid))
}

func killProcessGroup(pid int) {
	if pid <= 0 {
		return
	}
	taskkill("/T", "/F", "/PID", strconv.Itoa(pid))
}

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	h, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return errors.Is(err, syscall.ERROR_ACCESS_DENIED)
	}
	defer syscall.CloseHandle(h)
	var code uint32
	if err := syscall.GetExitCodeProcess(h, &code); err != nil {
		return true
	}
	return code == stillActive
}

func childPathEnv(hasPath bool) string {
	return ""
}

func openNoFollow(p string) (*os.File, error) {
	info, err := os.Lstat(p)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeIrregular != 0 {
		return nil, fmt.Errorf("refusing to follow link %s", p)
	}
	return os.Open(p)
}
