//go:build !windows

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

func configureChildProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
}

func terminateProcessGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGTERM)
}

func killProcessGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGKILL)
}

func isProcessAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	defer process.Release()
	if err := process.Signal(syscall.Signal(0)); err != nil {
		return !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH)
	}
	if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
		if end := strings.LastIndexByte(string(data), ')'); end >= 0 && end+2 < len(data) && data[end+2] == 'Z' {
			return false
		}
	}
	return true
}

func childPathEnv(hasPath bool) string {
	if !hasPath {
		return "PATH=/home/ubuntu/.local/bin:/usr/local/bin:/usr/bin:/bin"
	}
	return "PATH=/home/ubuntu/.local/bin:" + os.Getenv("PATH")
}

func openNoFollow(p string) (*os.File, error) {
	return os.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}
