package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRoleJobThatChangedProviderReportsFallback(t *testing.T) {
	job := &Job{ID: "sub_role_switch", Status: "done", ModelRequested: "@smol", ThinkingRequested: "low"}
	observeLines(t, job,
		`{"type":"response","command":"get_state","success":true,"data":{"model":{"provider":"anthropic","id":"claude-haiku-5-5"},"thinkingLevel":"xhigh","modelReport":{"requested":"@smol","ran":"anthropic/claude-haiku-5-5","effort":"xhigh","effortRequested":"low","switches":[{"from":"factory-droid/claude-haiku-5-5","to":"factory-droid/deepseek-v4.1-flash-1017","effortFrom":"low","effortTo":"max","reason":"usage reserve 1%","kind":"usage"},{"from":"factory-droid/deepseek-v4.1-flash-1017","to":"anthropic/claude-haiku-5-5","effortFrom":"max","effortTo":"xhigh","reason":"usage reserve 1%","kind":"usage"}],"policyViolations":["report: effort low raised to xhigh"]}}}`,
	)
	out := map[string]any{}
	job.addExecutionMetadataLocked(out)
	if out["fallback"] != true || out["fallback_from"] != "factory-droid/claude-haiku-5-5:low" || out["fallback_reason"] != "usage reserve 1%" {
		t.Fatalf("fallback = %v", out)
	}
	if switches, _ := out["model_switches"].([]modelSwitch); len(switches) != 2 || switches[1].To != "anthropic/claude-haiku-5-5" {
		t.Fatalf("model_switches = %v", out["model_switches"])
	}
	if got, _ := out["policy_violations"].([]string); len(got) != 1 {
		t.Fatalf("policy_violations = %v", out["policy_violations"])
	}
	want := `<model requested="@smol:low" ran="anthropic/claude-haiku-5-5" effort="xhigh" fallback="factory-droid/claude-haiku-5-5 -&gt; factory-droid/deepseek-v4.1-flash-1017: usage reserve 1%; -&gt; anthropic/claude-haiku-5-5: usage reserve 1%" effort-requested="low" policy="report: effort low raised to xhigh"/>`
	if out["model_line"] != want {
		t.Fatalf("model_line =\n%v\nwant\n%v", out["model_line"], want)
	}
}

func TestModelReportReplacesExactIDInference(t *testing.T) {
	reported := &Job{ID: "sub_route", ModelRequested: "anthropic/claude-opus-5-5"}
	observeLines(t, reported,
		`{"type":"response","command":"get_state","success":true,"data":{"model":{"provider":"bifrost","id":"omp-gw/anthropic/claude-opus-5-5"},"thinkingLevel":"high","modelReport":{"requested":"anthropic/claude-opus-5-5","ran":"bifrost/omp-gw/anthropic/claude-opus-5-5","effort":"high","switches":[]}}}`,
	)
	if reported.Fallback || !reported.ModelReportSeen {
		t.Fatalf("reported job: fallback %v seen %v", reported.Fallback, reported.ModelReportSeen)
	}
	if line := reported.modelLineLocked(); line != `<model requested="anthropic/claude-opus-5-5" ran="bifrost/omp-gw/anthropic/claude-opus-5-5" effort="high"/>` {
		t.Fatalf("model_line = %s", line)
	}

	old := &Job{ID: "sub_old_route", ModelRequested: "anthropic/claude-opus-5-5"}
	observeLines(t, old,
		`{"type":"response","command":"get_state","success":true,"data":{"model":{"provider":"bifrost","id":"omp-gw/anthropic/claude-opus-5-5"},"thinkingLevel":"high"}}`,
	)
	if !old.Fallback || old.ModelReportSeen {
		t.Fatalf("old child: fallback %v seen %v", old.Fallback, old.ModelReportSeen)
	}
}

func TestLocalChildGetsModelPolicyEnv(t *testing.T) {
	tempDir := newTestStores(t)
	path := filepath.Join(tempDir, "omp")
	script := "#!/bin/sh\necho \"policy=$OMP_MODEL_POLICY\" >> " + filepath.Join(tempDir, "env.log") + "\nexit 0\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OMP_PATH", path)
	t.Setenv("OMP_MODEL_POLICY", "off")

	job, err := startJob(jobSpec{Prompt: "env", CWD: tempDir})
	if err != nil {
		t.Fatalf("startJob: %v", err)
	}
	waitForJobSettled(t, job.ID)
	got, err := os.ReadFile(filepath.Join(tempDir, "env.log"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != "policy=enforce" {
		t.Fatalf("child env = %q", got)
	}
}
