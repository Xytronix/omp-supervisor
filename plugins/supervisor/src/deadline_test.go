package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func launchSleepingJob(t *testing.T, maxTime string) (*JobStore, *Job, string) {
	t.Helper()
	oldStore, oldSchedules := globalStore, globalSchedules
	base := t.TempDir()
	store, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	globalStore, globalSchedules = store, nil
	t.Cleanup(func() { globalStore, globalSchedules = oldStore, oldSchedules })
	bin := filepath.Join(base, "omp")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$0.args\"\nexec sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OMP_BIN", bin)
	t.Setenv("HOME", base)
	res, receipt := callTool(t, handleRun, map[string]any{"prompt": "work", "model": "provider/model", "cwd": base, "max_time": maxTime})
	if res.IsError {
		t.Fatalf("launch failed: %v", receipt)
	}
	if receipt["max_time"] != maxTime || receipt["deadline"] == nil {
		t.Fatalf("receipt lacks max_time/deadline: %v", receipt)
	}
	job := store.get(receipt["job_id"].(string))
	t.Cleanup(func() {
		job.mu.RLock()
		pid := job.PID
		job.mu.RUnlock()
		killProcessGroup(pid)
		select {
		case <-job.monitorDone:
		case <-time.After(10 * time.Second):
			t.Error("job monitor did not finish")
		}
	})
	return store, job, base
}

func jobDeadline(job *Job) (string, time.Time, time.Time) {
	job.mu.RLock()
	defer job.mu.RUnlock()
	return job.MaxTime, job.Deadline, job.StartTime
}

func TestExtendMovesDeadlinePastOriginalLimit(t *testing.T) {
	_, job, base := launchSleepingJob(t, "2s")
	id := job.ID
	var args []byte
	for range 50 {
		if args, _ = os.ReadFile(filepath.Join(base, "omp.args")); len(args) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(string(args), "--max-time=24h\n") {
		t.Fatalf("child did not get the 24h ceiling instead of the job's limit: %q", args)
	}
	_, deadline, start := jobDeadline(job)
	if !deadline.Equal(start.Add(2 * time.Second)) {
		t.Fatalf("deadline %v is not start+2s (%v)", deadline, start)
	}

	res, out := callTool(t, handleExtend, map[string]any{"job_id": id, "add": "3s"})
	if res.IsError || out["max_time"] != "5s" || out["job_id"] != id {
		t.Fatalf("extend add: %v", out)
	}
	if want := start.Add(5 * time.Second).UTC().Format(time.RFC3339); out["deadline"] != want {
		t.Fatalf("extend add deadline %v, want %s", out["deadline"], want)
	}
	res, out = callTool(t, handleExtend, map[string]any{"job_id": id, "max_time": "4s"})
	if res.IsError || out["max_time"] != "4s" {
		t.Fatalf("extend max_time: %v", out)
	}
	maxTime, deadline, _ := jobDeadline(job)
	if maxTime != "4s" || !deadline.Equal(start.Add(4*time.Second)) {
		t.Fatalf("job holds max_time=%s deadline=%v", maxTime, deadline)
	}

	reloaded, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	copy := reloaded.get(id)
	copy.mu.Lock()
	if copy.deadlineTimer != nil {
		copy.deadlineTimer.Stop()
	}
	if copy.MaxTime != "4s" || !copy.Deadline.Equal(deadline) {
		t.Fatalf("reload lost the extension: max_time=%s deadline=%v", copy.MaxTime, copy.Deadline)
	}
	copy.mu.Unlock()
	if row := reloaded.retainedJobs(10, 0).Jobs[0]; row.MaxTime != "4s" || row.Deadline != deadline.UTC().Format(time.RFC3339) {
		t.Fatalf("jobs row lacks the extension: %+v", row)
	}

	time.Sleep(time.Until(start.Add(2500 * time.Millisecond)))
	job.mu.RLock()
	status, pid := job.Status, job.PID
	job.mu.RUnlock()
	if status != "running" || !isProcessAlive(pid) {
		t.Fatalf("job stopped at its original limit: status=%s alive=%v", status, isProcessAlive(pid))
	}

	select {
	case <-job.monitorDone:
	case <-time.After(8 * time.Second):
		t.Fatal("job was not stopped at its extended deadline")
	}
	job.mu.RLock()
	status, exitCode, ended := job.Status, job.ExitCode, job.EndTime
	logPath := job.LogPath
	job.mu.RUnlock()
	if status != "error" || exitCode != 124 {
		t.Fatalf("expired job status=%s exit=%d, want error/124", status, exitCode)
	}
	if ran := ended.Sub(start); ran < 4*time.Second || ran > 7*time.Second {
		t.Fatalf("job ran %v, want about 4s", ran)
	}
	if data, _ := os.ReadFile(logPath); !strings.Contains(string(data), "Agent deadline exceeded") {
		t.Fatalf("log lacks the deadline message: %q", data)
	}

	res, out = callTool(t, handleExtend, map[string]any{"job_id": id, "add": "1h"})
	if !res.IsError || !strings.Contains(out["__error_text"].(string), "extend only works while a job is running") {
		t.Fatalf("extend on a terminal job: %v", out)
	}
}

func TestExtendRejectsInvalidRequests(t *testing.T) {
	_, job, _ := launchSleepingJob(t, "1h")
	time.Sleep(1100 * time.Millisecond)
	for _, tc := range []struct {
		args map[string]any
		want string
	}{
		{map[string]any{"job_id": "sub_missing", "add": "1h"}, "not found"},
		{map[string]any{"job_id": job.ID}, "exactly one"},
		{map[string]any{"job_id": job.ID, "add": "1h", "max_time": "2h"}, "exactly one"},
		{map[string]any{"job_id": job.ID, "add": "1.5h"}, "invalid duration"},
		{map[string]any{"job_id": job.ID, "max_time": "25h"}, "1 second to 24 hours"},
		{map[string]any{"job_id": job.ID, "add": "23h1m"}, "invalid duration"},
		{map[string]any{"job_id": job.ID, "add": "24h"}, "1 second to 24 hours"},
		{map[string]any{"job_id": job.ID, "max_time": "1s"}, "already in the past"},
	} {
		res, out := callTool(t, handleExtend, tc.args)
		text, _ := out["__error_text"].(string)
		if !res.IsError || !strings.Contains(text, tc.want) {
			t.Errorf("extend %v: got %v, want error containing %q", tc.args, out, tc.want)
		}
	}
	if maxTime, _, _ := jobDeadline(job); maxTime != "1h" {
		t.Fatalf("rejected extensions changed max_time to %s", maxTime)
	}
}

func TestExtendShortensDeadline(t *testing.T) {
	_, job, _ := launchSleepingJob(t, "1h")
	res, out := callTool(t, handleExtend, map[string]any{"job_id": job.ID, "max_time": "2s"})
	if res.IsError || out["max_time"] != "2s" {
		t.Fatalf("extend down: %v", out)
	}
	select {
	case <-job.monitorDone:
	case <-time.After(8 * time.Second):
		t.Fatal("job was not stopped at its shortened deadline")
	}
	job.mu.RLock()
	defer job.mu.RUnlock()
	if job.Status != "error" || job.ExitCode != 124 {
		t.Fatalf("status=%s exit=%d, want error/124", job.Status, job.ExitCode)
	}
}

func TestRunRejectsOutOfRangeMaxTime(t *testing.T) {
	for _, value := range []string{"0s", "25h", "10"} {
		if _, err := parseMaxTime(value); err == nil {
			t.Errorf("parseMaxTime(%q) accepted", value)
		}
	}
	for value, want := range map[string]time.Duration{"1s": time.Second, "1500ms": 1500 * time.Millisecond, "90m": 90 * time.Minute, "24h": 24 * time.Hour} {
		if got, err := parseMaxTime(value); err != nil || got != want {
			t.Errorf("parseMaxTime(%q) = %v, %v", value, got, err)
		}
	}
	if got := formatMaxTime(90 * time.Minute); got != "90m" {
		t.Errorf("formatMaxTime(90m) = %s", got)
	}
}

func TestDeadlineStopsSteerableJob(t *testing.T) {
	oldStore, oldSchedules := globalStore, globalSchedules
	base := t.TempDir()
	store, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	globalStore, globalSchedules = store, nil
	t.Cleanup(func() { globalStore, globalSchedules = oldStore, oldSchedules })
	bin := filepath.Join(base, "omp")
	script := "#!/bin/sh\nprintf '{\"type\":\"ready\"}\\n'\nwhile IFS= read -r line; do printf '{\"type\":\"agent_start\"}\\n'; done\n"
	if err := os.WriteFile(bin, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OMP_BIN", bin)
	t.Setenv("HOME", base)
	res, receipt := callTool(t, handleRun, map[string]any{"prompt": "work", "model": "provider/model", "cwd": base, "steerable": true, "max_time": "1s"})
	if res.IsError {
		t.Fatalf("launch failed: %v", receipt)
	}
	job := store.get(receipt["job_id"].(string))
	select {
	case <-job.monitorDone:
	case <-time.After(15 * time.Second):
		t.Fatal("steerable job was not stopped at its deadline")
	}
	_, status := callTool(t, handleStatus, map[string]any{"job_id": job.ID})
	if status["status"] != "error" || status["termination_reason"] != "timed_out" || status["max_time"] != "1s" {
		t.Fatalf("status after deadline: %v", status)
	}
	_, result := callTool(t, handleResult, map[string]any{"job_id": job.ID})
	if result["exit_code"] != float64(124) || result["report"] != "Agent deadline exceeded" {
		t.Fatalf("result after deadline: %v", result)
	}
	if data, _ := os.ReadFile(job.LogPath + ".stderr"); !strings.Contains(string(data), "Agent deadline exceeded") {
		t.Fatalf("stderr log lacks the deadline message: %q", data)
	}
}
