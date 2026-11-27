//go:build !windows

package main

import (
	"os/exec"
	"syscall"
	"testing"
	"time"
)

func TestReapAfterStdinEOFKillsHungChild(t *testing.T) {
	cmd := exec.Command("sh", "-c", "trap '' TERM; while :; do sleep 1; done")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stdinClosed := make(chan struct{})
	exited := make(chan struct{})
	waitErr := make(chan error, 1)
	go func() { err := cmd.Wait(); close(exited); waitErr <- err }()
	go reapAfterStdinEOF("sub_hung", cmd.Process.Pid, stdinClosed, exited, 100*time.Millisecond)

	select {
	case <-exited:
		t.Fatal("child was signaled before stdin EOF")
	case <-time.After(300 * time.Millisecond):
	}
	close(stdinClosed)
	select {
	case err := <-waitErr:
		status, ok := err.(*exec.ExitError)
		if !ok || status.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
			t.Fatalf("hung child did not end by SIGKILL: %v", err)
		}
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		t.Fatal("hung child survived the grace period")
	}
}

func TestReapAfterStdinEOFLeavesExitedChildAlone(t *testing.T) {
	stdinClosed := make(chan struct{})
	exited := make(chan struct{})
	done := make(chan struct{})
	close(stdinClosed)
	close(exited)
	go func() { reapAfterStdinEOF("sub_clean", 1<<30, stdinClosed, exited, time.Hour); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reaper kept waiting after the child exited")
	}
}

func TestRPCExitGraceBounds(t *testing.T) {
	for raw, want := range map[string]time.Duration{"": defaultRPCExitGrace, "0": defaultRPCExitGrace, "x": defaultRPCExitGrace, "3601": defaultRPCExitGrace, "5": 5 * time.Second} {
		t.Setenv("SUPERVISOR_RPC_EXIT_GRACE_SECONDS", raw)
		if got := rpcExitGrace(); got != want {
			t.Errorf("%q: got %s want %s", raw, got, want)
		}
	}
}
