package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunWithoutModelLeavesDestinationDefault(t *testing.T) {
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")

	job, err := startJob(jobSpec{Prompt: "no model given", CWD: tempDir})
	if err != nil {
		t.Fatalf("startJob: %v", err)
	}
	waitForJobSettled(t, job.ID)

	job.mu.RLock()
	requested := job.ModelRequested
	job.mu.RUnlock()
	if requested != defaultModelSelector {
		t.Fatalf("model_requested = %q, want %q", requested, defaultModelSelector)
	}
	argv, err := os.ReadFile(filepath.Join(tempDir, "argv.log"))
	if err != nil {
		t.Fatalf("read argv: %v", err)
	}
	if strings.Contains(string(argv), "--model") || strings.Contains(string(argv), "--thinking") {
		t.Fatalf("omitted model must not pass --model/--thinking, got %q", argv)
	}
}

func TestRunWithRoleSelectorForwardsRole(t *testing.T) {
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")

	job, err := startJob(jobSpec{Prompt: "use the task role", CWD: tempDir, RawModel: "@task"})
	if err != nil {
		t.Fatalf("startJob: %v", err)
	}
	waitForJobSettled(t, job.ID)

	argv, err := os.ReadFile(filepath.Join(tempDir, "argv.log"))
	if err != nil {
		t.Fatalf("read argv: %v", err)
	}
	if !strings.Contains(string(argv), "--model=@task") {
		t.Fatalf("expected --model=@task, got %q", argv)
	}
	if strings.Contains(string(argv), "--thinking") {
		t.Fatalf("role selector must keep the role's configured effort, got %q", argv)
	}
}

func TestRoleSelectorResolutionIsNotFallback(t *testing.T) {
	for _, requested := range []string{"@task", "@default", "@slow:xhigh"} {
		job := &Job{ModelRequested: requested}
		ok := true
		frame := rpcFrame{Type: "response", Command: "get_state", Success: &ok,
			Data: json.RawMessage(`{"model":{"provider":"bifrost","id":"omp-gw/openai-codex/gpt-6.1-sol"},"thinkingLevel":"max"}`)}
		if !job.observeExecutionLocked(frame) {
			t.Fatalf("%s: get_state was not observed", requested)
		}
		if job.Fallback {
			t.Errorf("%s resolved through its role must not be reported as fallback", requested)
		}
		if job.ModelResolved != "bifrost/omp-gw/openai-codex/gpt-6.1-sol" {
			t.Errorf("%s: model_resolved = %q", requested, job.ModelResolved)
		}
	}

	exact := &Job{ModelRequested: "bifrost/omp-gw/anthropic/claude-opus-5-5:high"}
	ok := true
	exact.observeExecutionLocked(rpcFrame{Type: "response", Command: "get_state", Success: &ok,
		Data: json.RawMessage(`{"model":{"provider":"bifrost","id":"omp-gw/openai-codex/gpt-6.1-sol"}}`)})
	if !exact.Fallback {
		t.Errorf("an exact model replaced by another must still be reported as fallback")
	}
}

func TestScheduleInheritsRequestedSelector(t *testing.T) {
	for _, tc := range []struct{ requested, want string }{
		{"@task", "@task"},
		{"@default", "@default"},
		{"", "@default"},
	} {
		tempDir := newTestStores(t)
		fakeOmp(t, tempDir, "0")
		target := &Job{
			ID:             "sub_role_target",
			Status:         "running",
			PID:            os.Getpid(),
			CWD:            tempDir,
			ModelRequested: tc.requested,
			ModelResolved:  "bifrost/omp-gw/openai-codex/gpt-6.1-sol",
			Model:          "bifrost/omp-gw/openai-codex/gpt-6.1-sol",
			StartTime:      time.Now(),
		}
		globalStore.add(target)

		_, payload := callTool(t, handleSchedule, map[string]any{"job_id": target.ID, "prompt": "follow up", "resume": false})
		id, _ := payload["schedule_id"].(string)
		sch := globalSchedules.get(id)
		if sch == nil {
			t.Fatalf("requested %q: schedule missing: %v", tc.requested, payload)
		}
		if sch.Model != tc.want {
			t.Errorf("requested %q: follow-up model = %q, want %q", tc.requested, sch.Model, tc.want)
		}
		globalSchedules.cancel(id)
	}
}

func TestDeadlineExitReportsTimedOut(t *testing.T) {
	out := map[string]any{}
	(&Job{Status: "error", ExitCode: 124}).addExecutionMetadataLocked(out)
	if out["termination_reason"] != "timed_out" {
		t.Fatalf("exit 124 must report termination_reason timed_out, got %v", out["termination_reason"])
	}
	other := map[string]any{}
	(&Job{Status: "error", ExitCode: 1}).addExecutionMetadataLocked(other)
	if _, ok := other["termination_reason"]; ok {
		t.Fatalf("ordinary failures must not claim a timeout: %v", other)
	}
}

func TestRunExactModelWithoutThinkingPassesNoThinking(t *testing.T) {
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")

	job, err := startJob(jobSpec{Prompt: "exact model", CWD: tempDir, RawModel: "anthropic/claude-opus-5-5"})
	if err != nil {
		t.Fatalf("startJob: %v", err)
	}
	waitForJobSettled(t, job.ID)

	argv, err := os.ReadFile(filepath.Join(tempDir, "argv.log"))
	if err != nil {
		t.Fatalf("read argv: %v", err)
	}
	if !strings.Contains(string(argv), "--model=anthropic/claude-opus-5-5") {
		t.Fatalf("expected the exact model, got %q", argv)
	}
	if strings.Contains(string(argv), "--thinking") {
		t.Fatalf("an exact model without thinking must not pass --thinking, got %q", argv)
	}
	job.mu.RLock()
	requested := job.ThinkingRequested
	job.mu.RUnlock()
	if requested != "" {
		t.Fatalf("thinking_requested = %q, want empty", requested)
	}
}

func TestScheduleKeepsRequestedThinking(t *testing.T) {
	for _, tc := range []struct {
		name         string
		args         map[string]any
		wantModel    string
		wantThinking string
	}{
		{"inherited", map[string]any{}, "anthropic/claude-opus-5-5", "low"},
		{"explicit thinking wins", map[string]any{"thinking": "xhigh"}, "anthropic/claude-opus-5-5", "xhigh"},
		{"new model has its own default", map[string]any{"model": "@task"}, "@task", ""},
	} {
		tempDir := newTestStores(t)
		fakeOmp(t, tempDir, "0")
		target := &Job{
			ID:                "sub_thinking_target",
			Status:            "running",
			PID:               os.Getpid(),
			CWD:               tempDir,
			ModelRequested:    "anthropic/claude-opus-5-5",
			ThinkingRequested: "low",
			StartTime:         time.Now(),
		}
		globalStore.add(target)

		args := map[string]any{"job_id": target.ID, "prompt": "follow up", "resume": false}
		for k, v := range tc.args {
			args[k] = v
		}
		_, payload := callTool(t, handleSchedule, args)
		id, _ := payload["schedule_id"].(string)
		sch := globalSchedules.get(id)
		if sch == nil {
			t.Fatalf("%s: schedule missing: %v", tc.name, payload)
		}
		if sch.Model != tc.wantModel || sch.Thinking != tc.wantThinking {
			t.Errorf("%s: follow-up = %q/%q, want %q/%q", tc.name, sch.Model, sch.Thinking, tc.wantModel, tc.wantThinking)
		}
		if spec := sch.spec(); spec.Thinking != tc.wantThinking {
			t.Errorf("%s: spec thinking = %q", tc.name, spec.Thinking)
		}
		globalSchedules.cancel(id)
	}
}
