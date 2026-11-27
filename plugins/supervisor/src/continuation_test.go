package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func isolateMachines(t *testing.T) {
	t.Helper()
	t.Setenv("SUPERVISOR_MACHINE", "control")
	t.Setenv("SUPERVISOR_REMOTE_MACHINES_FILE", filepath.Join(t.TempDir(), "machines.json"))
}

func seedTerminalJob(id, sessionID string, steerable bool) *Job {
	job := &Job{
		ID:                id,
		Status:            "done",
		Machine:           "control",
		CWD:               os.TempDir(),
		SessionID:         sessionID,
		Steerable:         steerable,
		ModelRequested:    "@task",
		ThinkingRequested: "high",
		MaxTime:           "10m",
		Title:             "seeded work",
		Yolo:              true,
		StartTime:         time.Now().Add(-time.Hour),
		EndTime:           time.Now().Add(-time.Minute),
	}
	globalStore.add(job)
	return job
}

func TestSteerToFinishedJobContinuesSession(t *testing.T) {
	isolateMachines(t)
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")
	orig := seedTerminalJob("sub_orig", "sess-1", true)

	_, payload := callTool(t, handleSteer, map[string]any{"job_id": orig.ID, "text": "keep going"})
	if payload["continued"] != true {
		t.Fatalf("steer into a finished job did not continue it: %v", payload)
	}
	childID, _ := payload["job_id"].(string)
	if childID == "" || childID == orig.ID {
		t.Fatalf("continuation must be a new job, got %v", payload)
	}
	if payload["continued_job_id"] != orig.ID {
		t.Fatalf("continued_job_id = %v, want %s", payload["continued_job_id"], orig.ID)
	}
	if payload["session_id"] != "sess-1" {
		t.Fatalf("continuation session_id = %v, want sess-1", payload["session_id"])
	}
	waitForJobSettled(t, childID)

	child := globalStore.get(childID)
	child.mu.RLock()
	if child.ContinuedFromJobID != orig.ID {
		t.Errorf("child continued_from = %q, want %q", child.ContinuedFromJobID, orig.ID)
	}
	if child.Prompt != "keep going" {
		t.Errorf("child prompt = %q, want the delivered text", child.Prompt)
	}
	inherited := map[string]string{
		"model":     child.ModelRequested,
		"thinking":  child.ThinkingRequested,
		"max_time":  child.MaxTime,
		"session":   child.SessionID,
		"cwd_empty": child.CWD,
	}
	child.mu.RUnlock()
	if inherited["model"] != "@task" || inherited["thinking"] != "high" || inherited["max_time"] != "10m" {
		t.Errorf("continuation must inherit model/effort/max_time: %+v", inherited)
	}
	orig.mu.RLock()
	by := orig.ContinuedByJobID
	orig.mu.RUnlock()
	if by != childID {
		t.Errorf("original continued_by = %q, want %q", by, childID)
	}

	argv, err := os.ReadFile(filepath.Join(tempDir, "argv.log"))
	if err != nil {
		t.Fatalf("read argv: %v", err)
	}
	if !strings.Contains(string(argv), "-r sess-1") {
		t.Errorf("continuation must resume with -r sess-1, got %q", argv)
	}
	if !strings.Contains(string(argv), "--model=@task") {
		t.Errorf("continuation must inherit the requested model, got %q", argv)
	}
}

func TestContinuationWalksForwardToChainTail(t *testing.T) {
	isolateMachines(t)
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")
	seedTerminalJob("sub_c0", "sess-chain", true)

	_, first := callTool(t, handleSteer, map[string]any{"job_id": "sub_c0", "text": "second turn"})
	firstID, _ := first["job_id"].(string)
	waitForJobSettled(t, firstID)

	_, second := callTool(t, handleSteer, map[string]any{"job_id": "sub_c0", "text": "third turn"})
	if second["continued"] != true {
		t.Fatalf("second steer did not continue: %v", second)
	}
	secondID, _ := second["job_id"].(string)
	waitForJobSettled(t, secondID)
	if secondID == firstID {
		t.Fatalf("chain tail was already terminal, so a new continuation is required: %v", second)
	}
	if second["continued_job_id"] != firstID {
		t.Fatalf("continuation must extend the chain tail %s, not the requested link: %v", firstID, second)
	}
	if second["requested_job_id"] != "sub_c0" {
		t.Fatalf("requested_job_id must record the older link the caller addressed: %v", second)
	}

	mid := globalStore.get(firstID)
	mid.mu.RLock()
	from, by := mid.ContinuedFromJobID, mid.ContinuedByJobID
	mid.mu.RUnlock()
	if from != "sub_c0" || by != secondID {
		t.Fatalf("middle link lineage = from %q by %q, want from sub_c0 by %s", from, by, secondID)
	}
}

func TestContinuationIntoRunningTailDoesNotDoubleResume(t *testing.T) {
	isolateMachines(t)
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "2")
	orig := seedTerminalJob("sub_rc0", "sess-race", true)

	_, first := callTool(t, handleSteer, map[string]any{"job_id": orig.ID, "text": "second turn"})
	firstID, _ := first["job_id"].(string)
	waitForJobStatus(t, firstID, "running", 5*time.Second)

	before := len(globalStore.retainedJobs(200, 0).Jobs)
	callTool(t, handleSteer, map[string]any{"job_id": orig.ID, "text": "third turn"})
	after := len(globalStore.retainedJobs(200, 0).Jobs)
	if after != before {
		t.Fatalf("delivery to a chain with a running tail launched another job (%d -> %d)", before, after)
	}

	resumes := 0
	for _, row := range globalStore.retainedJobs(200, 0).Jobs {
		if row.SessionID == "sess-race" && row.JobID != orig.ID {
			resumes++
		}
	}
	if resumes != 1 {
		t.Fatalf("sess-race resumed %d times, want exactly 1", resumes)
	}
	waitForJobSettled(t, firstID)
}

func TestContinuationRequiresAResumableSession(t *testing.T) {
	isolateMachines(t)
	newTestStores(t)
	job := &Job{ID: "sub_nosess", Status: "error", StartTime: time.Now().Add(-time.Hour), EndTime: time.Now()}
	globalStore.add(job)

	res, payload := callTool(t, handleSteer, map[string]any{"job_id": job.ID, "text": "again"})
	if !res.IsError {
		t.Fatalf("continuing a sessionless job must fail: %v", payload)
	}
	if !strings.Contains(payload["__error_text"].(string), "no resumable OMP session") {
		t.Fatalf("error must explain the missing session: %v", payload["__error_text"])
	}
}

func TestContinuationOfPrintModeJob(t *testing.T) {
	isolateMachines(t)
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")
	orig := seedTerminalJob("sub_pm", "sess-print", false)

	_, payload := callTool(t, handleSteer, map[string]any{"job_id": orig.ID, "text": "one more"})
	if payload["continued"] != true {
		t.Fatalf("print-mode terminal job was not continued: %v", payload)
	}
	childID, _ := payload["job_id"].(string)
	waitForJobSettled(t, childID)
	child := globalStore.get(childID)
	child.mu.RLock()
	steerable := child.Steerable
	child.mu.RUnlock()
	if steerable {
		t.Errorf("continuation of a print-mode job must stay print-mode")
	}
}

func TestJobsListingExposesSessionAndLineage(t *testing.T) {
	isolateMachines(t)
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")
	orig := seedTerminalJob("sub_li0", "sess-list", true)

	_, payload := callTool(t, handleSteer, map[string]any{"job_id": orig.ID, "text": "more"})
	childID, _ := payload["job_id"].(string)
	waitForJobSettled(t, childID)

	_, listPayload := callTool(t, handleJobs, map[string]any{})
	rows, _ := listPayload["jobs"].([]any)
	byID := map[string]map[string]any{}
	for _, raw := range rows {
		row, _ := raw.(map[string]any)
		id, _ := row["job_id"].(string)
		byID[id] = row
	}
	origRow := byID[orig.ID]
	if origRow == nil || origRow["session_id"] != "sess-list" {
		t.Fatalf("jobs row for the original lacks the resume handle: %v", origRow)
	}
	if origRow["continued_by_job_id"] != childID {
		t.Fatalf("jobs row lacks the forward lineage link: %v", origRow)
	}
	childRow := byID[childID]
	if childRow == nil || childRow["continued_from_job_id"] != orig.ID {
		t.Fatalf("continuation row lacks the backward lineage link: %v", childRow)
	}
}

func TestStatusExposesContinuationLineage(t *testing.T) {
	isolateMachines(t)
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")
	orig := seedTerminalJob("sub_st0", "sess-st", true)

	_, payload := callTool(t, handleSteer, map[string]any{"job_id": orig.ID, "text": "more"})
	childID, _ := payload["job_id"].(string)
	waitForJobSettled(t, childID)

	_, statusPayload := callTool(t, handleStatus, map[string]any{"job_id": orig.ID})
	if statusPayload["continued_by_job_id"] != childID {
		t.Fatalf("status lacks continued_by_job_id: %v", statusPayload)
	}
	_, childStatus := callTool(t, handleStatus, map[string]any{"job_id": childID})
	if childStatus["continued_from_job_id"] != orig.ID {
		t.Fatalf("continuation status lacks continued_from_job_id: %v", childStatus)
	}
}
