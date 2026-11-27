package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	loadMachinePolicy = func(context.Context, *remoteMachine) (*modelPolicy, error) { return nil, nil }
	os.Exit(m.Run())
}

func stubMachinePolicy(t *testing.T, load func(context.Context, *remoteMachine) (*modelPolicy, error)) {
	t.Helper()
	original := loadMachinePolicy
	reset := func() {
		machinePolicyMu.Lock()
		machinePolicyCache = map[string]machinePolicyEntry{}
		machinePolicyMu.Unlock()
	}
	reset()
	loadMachinePolicy = load
	t.Cleanup(func() {
		loadMachinePolicy = original
		reset()
	})
}

type policyCaseFile struct {
	Policy json.RawMessage `json:"policy"`
	Cases  []struct {
		Name     string `json:"name"`
		Model    string `json:"model"`
		Thinking string `json:"thinking"`
		Allow    bool   `json:"allow"`
	} `json:"cases"`
}

func TestModelPolicyCases(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "model-policy-cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file policyCaseFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("case file: %v", err)
	}
	policy := parseModelPolicy(file.Policy)
	if !policy.active() {
		t.Fatalf("case file policy is not an active policy: %s", file.Policy)
	}
	if len(file.Cases) == 0 {
		t.Fatal("case file has no cases")
	}
	for _, tc := range file.Cases {
		reason := policy.check(tc.Model, tc.Thinking)
		if (reason == "") != tc.Allow {
			t.Errorf("%s: check(%q, %q) = %q, want allow=%v", tc.Name, tc.Model, tc.Thinking, reason, tc.Allow)
		}
	}
}

func TestModelPolicyInactiveChecksNothing(t *testing.T) {
	for _, policy := range []*modelPolicy{
		nil,
		{Configured: false, Mode: "enforce"},
		{Configured: true, Mode: "off"},
	} {
		if reason := policy.check("openai-codex/gpt-6-luna", "low"); reason != "" {
			t.Errorf("%+v: reason = %q", policy, reason)
		}
	}
}

func TestParseModelPolicyRejectsNonPolicy(t *testing.T) {
	for _, raw := range []string{``, `not json`, `{"models":[]}`} {
		if parseModelPolicy([]byte(raw)) != nil {
			t.Errorf("%q parsed as a policy", raw)
		}
	}
	if p := parseModelPolicy([]byte(`{"configured":true,"mode":"report","models":[],"roles":[]}`)); p == nil || p.Mode != "report" {
		t.Fatalf("policy = %+v", p)
	}
}

var enforcedPolicy = &modelPolicy{
	Configured: true, Mode: "enforce",
	Models: []policyModel{{ID: "anthropic/claude-opus-5-5", Efforts: []string{"medium", "high"}}},
	Roles:  []policyRole{{Role: "task", Model: "anthropic/claude-opus-5-5", Effort: "medium", InPolicy: true, Guide: "code that ships"}},
}

func TestEnforceRefusesWithMenu(t *testing.T) {
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")
	stubMachinePolicy(t, func(context.Context, *remoteMachine) (*modelPolicy, error) { return enforcedPolicy, nil })

	_, err := startJob(jobSpec{Prompt: "refused", CWD: tempDir, RawModel: "anthropic/claude-opus-5-5", Thinking: "low"})
	if err == nil {
		t.Fatal("enforce mode launched an out-of-policy effort")
	}
	for _, want := range []string{"refused by modelPolicy", "allows medium|high, not low", "@task anthropic/claude-opus-5-5:medium: code that ships", "anthropic/claude-opus-5-5 (medium|high)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lacks %q", err, want)
		}
	}
	if _, statErr := os.Stat(filepath.Join(tempDir, "argv.log")); statErr == nil {
		t.Fatal("a refused job must not start omp")
	}
}

func TestReportModeLaunchesAndRecordsViolation(t *testing.T) {
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")
	report := *enforcedPolicy
	report.Mode = "report"
	stubMachinePolicy(t, func(context.Context, *remoteMachine) (*modelPolicy, error) { return &report, nil })

	job, err := startJob(jobSpec{Prompt: "reported", CWD: tempDir, RawModel: "@task:low"})
	if err != nil {
		t.Fatalf("report mode refused: %v", err)
	}
	waitForJobSettled(t, job.ID)
	job.mu.RLock()
	out := map[string]any{}
	job.addExecutionMetadataLocked(out)
	job.mu.RUnlock()
	got, _ := out["policy_violations"].([]string)
	if len(got) != 1 || !strings.HasPrefix(got[0], "report: @task:low: ") || !strings.Contains(got[0], "not low") {
		t.Fatalf("policy_violations = %v", out["policy_violations"])
	}

	clean, err := startJob(jobSpec{Prompt: "in policy", CWD: tempDir, RawModel: "@task"})
	if err != nil {
		t.Fatal(err)
	}
	waitForJobSettled(t, clean.ID)
	if len(clean.PolicyViolations) != 0 {
		t.Fatalf("in-policy job reported %v", clean.PolicyViolations)
	}
}

func TestPolicyUnavailableSkipsCheck(t *testing.T) {
	tempDir := newTestStores(t)
	fakeOmp(t, tempDir, "0")
	calls := 0
	stubMachinePolicy(t, func(context.Context, *remoteMachine) (*modelPolicy, error) {
		calls++
		return nil, errors.New("ssh: connect refused")
	})
	for i := 0; i < 2; i++ {
		job, err := startJob(jobSpec{Prompt: "old fleet", CWD: tempDir, RawModel: "openai-codex/gpt-6-luna", Thinking: "low"})
		if err != nil {
			t.Fatalf("unavailable policy blocked the launch: %v", err)
		}
		waitForJobSettled(t, job.ID)
		if len(job.PolicyViolations) != 0 {
			t.Fatalf("violations = %v", job.PolicyViolations)
		}
	}
	if calls != 1 {
		t.Fatalf("policy loads = %d, want 1 (cached)", calls)
	}
}
