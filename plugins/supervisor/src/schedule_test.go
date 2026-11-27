package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fakeOmp(t *testing.T, dir string, sleepSeconds string) string {
	t.Helper()
	path := filepath.Join(dir, "omp")
	script := "#!/bin/sh\necho \"argv: $@\" >> " + filepath.Join(dir, "argv.log") + "\nsleep " + sleepSeconds + "\nexit 0\n"
	if err := os.WriteFile(path, []byte(script), 0755); err != nil {
		t.Fatalf("write fake omp: %v", err)
	}
	t.Setenv("OMP_PATH", path)
	return path
}

func waitForJobSettled(t *testing.T, jobID string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		job := globalStore.get(jobID)
		if job == nil {
			return
		}
		job.mu.RLock()
		status := job.Status
		job.mu.RUnlock()
		if status != "running" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("job %s never settled", jobID)
}

func waitForJobStatus(t *testing.T, jobID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if job := globalStore.get(jobID); job != nil {
			job.mu.RLock()
			status := job.Status
			job.mu.RUnlock()
			if status == want {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("job %s never reached status %q", jobID, want)
}

func TestScheduleStartsChildWhenTargetFinishes(t *testing.T) {
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "1")

	target, err := startJob(jobSpec{Prompt: "long running work", CWD: tempDir})
	if err != nil {
		t.Fatalf("startJob: %v", err)
	}

	_, payload := callTool(t, handleSchedule, map[string]any{
		"job_id": target.ID,
		"prompt": "summarize what changed",
		"resume": false,
	})
	if payload["status"] != "pending" {
		t.Fatalf("expected a pending schedule while the target runs, got %v", payload)
	}
	scheduleID, _ := payload["schedule_id"].(string)
	if scheduleID == "" {
		t.Fatalf("missing schedule_id: %v", payload)
	}

	_, statusPayload := callTool(t, handleStatus, map[string]any{"job_id": target.ID})
	queued, _ := statusPayload["scheduled"].([]any)
	if len(queued) != 1 {
		t.Errorf("expected the queued follow-up in status, got %v", statusPayload["scheduled"])
	}

	waitForJobStatus(t, target.ID, "done", 15*time.Second)

	deadline := time.Now().Add(10 * time.Second)
	var childID string
	for time.Now().Before(deadline) {
		if sch := globalSchedules.get(scheduleID); sch != nil && sch.Status == "started" {
			childID = sch.ChildJobID
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if childID == "" {
		t.Fatalf("schedule %s never started a child: %+v", scheduleID, globalSchedules.get(scheduleID))
	}
	waitForJobSettled(t, childID)
	child := globalStore.get(childID)
	if child == nil {
		t.Fatalf("child job %s missing from the store", childID)
	}
	child.mu.RLock()
	defer child.mu.RUnlock()
	if child.Prompt != "summarize what changed" {
		t.Errorf("child ran the wrong prompt: %q", child.Prompt)
	}
	if child.ID == target.ID {
		t.Errorf("schedule must start a new job, not reuse the target")
	}
}

func TestScheduleOnFinishedJobStartsImmediately(t *testing.T) {
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")

	target := &Job{
		ID:        "sub_already_done",
		Status:    "done",
		CWD:       tempDir,
		SessionID: "01a037dd-8b65-7000-8000-000000000017",
		StartTime: time.Now().Add(-time.Minute),
		EndTime:   time.Now(),
	}
	globalStore.add(target)

	_, payload := callTool(t, handleSchedule, map[string]any{
		"job_id": target.ID,
		"prompt": "follow up now",
	})
	if payload["status"] != "started" {
		t.Fatalf("expected an immediate start for a terminal target, got %v", payload)
	}
	childID, _ := payload["child_job_id"].(string)
	waitForJobSettled(t, childID)
	child := globalStore.get(childID)
	if child == nil {
		t.Fatalf("child job %q missing: %v", childID, payload)
	}

	child.mu.RLock()
	defer child.mu.RUnlock()
	if child.SessionID != target.SessionID {
		t.Errorf("expected the follow-up to resume session %s, got %s", target.SessionID, child.SessionID)
	}
}

func TestSchedulePersistsAndFiresAfterRestart(t *testing.T) {
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")

	target := &Job{ID: "sub_restart_target", Status: "done", CWD: tempDir, StartTime: time.Now(), EndTime: time.Now()}
	globalStore.add(target)

	sch := &Schedule{
		ID:          "sch_persisted",
		TargetJobID: target.ID,
		Prompt:      "run after restart",
		Model:       defaultModelSelector,
		CWD:         tempDir,
		Status:      "pending",
		CreatedAt:   time.Now(),
	}
	globalSchedules.add(sch)

	reloaded, err := newScheduleStore(tempDir)
	if err != nil {
		t.Fatalf("reload schedules: %v", err)
	}
	globalSchedules = reloaded
	if got := reloaded.get("sch_persisted"); got == nil || got.Status != "pending" {
		t.Fatalf("schedule did not survive the restart: %+v", got)
	}

	reloaded.reconcile()

	fired := reloaded.get("sch_persisted")
	if fired == nil || fired.Status != "started" || fired.ChildJobID == "" {
		t.Fatalf("reconcile should start schedules whose target already finished: %+v", fired)
	}
	if globalStore.get(fired.ChildJobID) == nil {
		t.Errorf("child job %s not registered", fired.ChildJobID)
	}
	waitForJobSettled(t, fired.ChildJobID)
}

func TestScheduleFiresOnceAndCanBeCanceled(t *testing.T) {
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")

	target := &Job{ID: "sub_once", Status: "done", CWD: tempDir, StartTime: time.Now(), EndTime: time.Now()}
	globalStore.add(target)

	globalSchedules.add(&Schedule{
		ID:          "sch_once",
		TargetJobID: target.ID,
		Prompt:      "only once",
		CWD:         tempDir,
		Status:      "pending",
		CreatedAt:   time.Now(),
	})

	globalSchedules.onJobTerminal(target.ID)
	first := globalSchedules.get("sch_once")
	globalSchedules.onJobTerminal(target.ID)
	second := globalSchedules.get("sch_once")
	waitForJobSettled(t, second.ChildJobID)

	if first == nil || first.ChildJobID == "" {
		t.Fatalf("schedule never started: %+v", first)
	}
	if second.ChildJobID != first.ChildJobID {
		t.Errorf("repeated terminal notifications must not start a second child: %s vs %s", first.ChildJobID, second.ChildJobID)
	}

	globalSchedules.add(&Schedule{
		ID:          "sch_cancelable",
		TargetJobID: "sub_still_running",
		Prompt:      "never runs",
		CWD:         tempDir,
		Status:      "pending",
		CreatedAt:   time.Now(),
	})
	res, payload := callTool(t, handleCancel, map[string]any{"schedule_id": "sch_cancelable"})
	if res.IsError || payload["canceled"] != true {
		t.Fatalf("expected the schedule to be canceled: %v", payload)
	}
	globalSchedules.onJobTerminal("sub_still_running")
	if got := globalSchedules.get("sch_cancelable"); got.Status != "canceled" || got.ChildJobID != "" {
		t.Errorf("canceled schedule must stay canceled: %+v", got)
	}

	if _, err := os.Stat(filepath.Join(tempDir, "schedules", "sch_cancelable.json")); err != nil {
		t.Errorf("schedule state should be persisted: %v", err)
	}
}

func TestScheduleRequiresJobAndPrompt(t *testing.T) {
	newTestStores(t)

	if res, _ := callTool(t, handleSchedule, map[string]any{"prompt": "x"}); !res.IsError {
		t.Errorf("expected an error without job_id")
	}
	if res, _ := callTool(t, handleSchedule, map[string]any{"job_id": "sub_x"}); !res.IsError {
		t.Errorf("expected an error without a prompt")
	}
	if res, _ := callTool(t, handleSchedule, map[string]any{"job_id": "sub_missing", "prompt": "x"}); !res.IsError {
		t.Errorf("expected an error for an unknown target job")
	}
}

func TestScheduleResponseUsesPromptPreview(t *testing.T) {
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")

	target := &Job{ID: "sub_preview", Status: "done", CWD: tempDir, StartTime: time.Now(), EndTime: time.Now()}
	globalStore.add(target)

	longPrompt := strings.Repeat("p", 500)
	_, payload := callTool(t, handleSchedule, map[string]any{
		"job_id": target.ID,
		"prompt": longPrompt,
		"resume": false,
	})
	if _, ok := payload["prompt"]; ok {
		t.Fatalf("schedule must not echo the full prompt: %v", payload)
	}
	preview, _ := payload["prompt_preview"].(string)
	if preview != longPrompt[:promptPreviewMaxChars] {
		t.Fatalf("prompt_preview=%q want first %d chars", preview, promptPreviewMaxChars)
	}
	if asInt(payload["prompt_chars"]) != 500 {
		t.Fatalf("prompt_chars=%v", payload["prompt_chars"])
	}
	if payload["truncated"] != true {
		t.Fatalf("long prompt should be marked truncated: %v", payload)
	}
	if childID, _ := payload["child_job_id"].(string); childID != "" {
		waitForJobSettled(t, childID)
	}
}
