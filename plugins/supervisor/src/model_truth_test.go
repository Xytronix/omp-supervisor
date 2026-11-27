package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
)

func observeLines(t *testing.T, job *Job, lines ...string) {
	t.Helper()
	for _, line := range lines {
		frame, err := decodeFrame([]byte(line))
		if err != nil {
			t.Fatalf("decode %s: %v", line, err)
		}
		job.observeExecutionLocked(frame)
	}
}

func TestRoleRequestReportsRetryFallback(t *testing.T) {
	job := &Job{ID: "sub_fallback", Status: "done", ModelRequested: "@smol", ThinkingRequested: "low"}
	observeLines(t, job,
		`{"type":"response","command":"get_state","success":true,"data":{"model":{"provider":"factory-droid","id":"deepseek-v4.1-flash-1017"},"thinkingLevel":"max","retryFallback":{"role":"smol","originalSelector":"factory-droid/claude-haiku-5-5:low"}}}`,
		`{"type":"retry_fallback_applied","from":"factory-droid/deepseek-v4.1-flash-1017:max","to":"anthropic/claude-haiku-5-5:xhigh","role":"smol","reason":"Usage preflight: quota at reserve"}`,
		`{"type":"model_changed"}`,
		`{"type":"message_end","message":{"role":"assistant","provider":"factory-droid","model":"deepseek-v4.1-flash-1017","stopReason":"error","content":[]}}`,
		`{"type":"response","command":"get_state","success":true,"data":{"model":{"provider":"anthropic","id":"claude-haiku-5-5"},"thinkingLevel":"xhigh","retryFallback":{"role":"smol","originalSelector":"factory-droid/claude-haiku-5-5:low"}}}`,
		`{"type":"message_end","message":{"role":"assistant","provider":"anthropic","model":"claude-haiku-5-5","stopReason":"stop","content":[{"type":"text","text":"PONG"}]}}`,
		`{"type":"turn_end","message":{"role":"assistant","provider":"anthropic","model":"claude-haiku-5-5","stopReason":"stop","content":[]}}`,
	)
	out := map[string]any{}
	job.addExecutionMetadataLocked(out)
	if out["model_requested"] != "@smol" || out["model_resolved"] != "anthropic/claude-haiku-5-5" ||
		out["thinking_requested"] != "low" || out["thinking"] != "xhigh" {
		t.Fatalf("execution = %v", out)
	}
	if out["fallback"] != true || out["fallback_from"] != "factory-droid/claude-haiku-5-5:low" || out["fallback_reason"] != "Usage preflight: quota at reserve" {
		t.Fatalf("fallback = %v", out)
	}
	if !reflect.DeepEqual(out["models_used"], []string{"anthropic/claude-haiku-5-5"}) {
		t.Fatalf("models_used = %v", out["models_used"])
	}
}

func TestRetryFallbackEventWithoutStateOrigin(t *testing.T) {
	job := &Job{ID: "sub_old", Status: "running", ModelRequested: "@default"}
	observeLines(t, job,
		`{"type":"retry_fallback_applied","from":"factory-droid/claude-haiku-5-5:xhigh","to":"factory-droid/deepseek-v4.1-flash-1017:max","role":"default"}`,
		`{"type":"response","command":"get_state","success":true,"data":{"model":{"provider":"factory-droid","id":"deepseek-v4.1-flash-1017"},"thinkingLevel":"max"}}`,
	)
	if !job.Fallback || job.FallbackFrom != "factory-droid/claude-haiku-5-5:xhigh" || job.ModelResolved != "factory-droid/deepseek-v4.1-flash-1017" {
		t.Fatalf("job = fallback %v from %q model %q", job.Fallback, job.FallbackFrom, job.ModelResolved)
	}
}

func TestRoleRequestWithoutFallback(t *testing.T) {
	job := &Job{ID: "sub_clean", Status: "done", ModelRequested: "@task"}
	observeLines(t, job,
		`{"type":"response","command":"get_state","success":true,"data":{"model":{"provider":"anthropic","id":"claude-opus-5-5"},"thinkingLevel":"medium"}}`,
		`{"type":"message_end","message":{"role":"assistant","provider":"anthropic","model":"claude-opus-5-5","stopReason":"stop","content":[]}}`,
	)
	out := map[string]any{}
	job.addExecutionMetadataLocked(out)
	if out["fallback"] != false || out["fallback_from"] != nil {
		t.Fatalf("out = %v", out)
	}
}

func TestModelsReportsTargetMachineRoles(t *testing.T) {
	dir := t.TempDir()
	machinesFile := filepath.Join(dir, "machines.json")
	if err := os.WriteFile(machinesFile, []byte(`{"mac":{"sshHost":"mac","command":"node host.mjs","home":"/Users/x"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUPERVISOR_REMOTE_MACHINES_FILE", machinesFile)
	original := loadMachineModels
	t.Cleanup(func() {
		loadMachineModels = original
		machineModelsMu.Lock()
		machineModelsCache = map[string]machineModelsEntry{}
		machineModelsMu.Unlock()
	})
	var gotTarget *remoteMachine
	loadMachineModels = func(_ context.Context, target *remoteMachine, catalog bool) (machineModels, error) {
		gotTarget = target
		return machineModels{
			Roles:          map[string]string{"smol": "bifrost/omp-gw/anthropic/claude-haiku-5-5:low"},
			FallbackChains: map[string][]string{"smol": {"bifrost/omp-gw/factory-droid/deepseek-v4.1-flash-1017:max"}},
			Catalog: []ompModel{{Provider: "bifrost", ID: "omp-gw/anthropic/claude-haiku-5-5", Selector: "bifrost/omp-gw/anthropic/claude-haiku-5-5",
				Thinking: []string{"low", "high"}}},
		}, nil
	}
	req := mcp.CallToolRequest{}
	req.Params.Name = "models"
	req.Params.Arguments = map[string]any{"machine": "mac", "query": "haiku"}
	res, err := handleModels(context.Background(), req)
	if err != nil || res.IsError {
		t.Fatalf("handleModels: %v %s", err, toolResultText(res))
	}
	if gotTarget == nil || gotTarget.SSHHost != "mac" {
		t.Fatalf("inventory target = %+v", gotTarget)
	}
	var out struct {
		Machine        string              `json:"machine"`
		Roles          map[string]string   `json:"roles"`
		FallbackChains map[string][]string `json:"fallback_chains"`
		Models         []ompModel          `json:"models"`
	}
	if err := json.Unmarshal([]byte(res.Content[0].(mcp.TextContent).Text), &out); err != nil {
		t.Fatal(err)
	}
	if out.Machine != "mac" || out.Roles["smol"] != "bifrost/omp-gw/anthropic/claude-haiku-5-5:low" ||
		len(out.FallbackChains["smol"]) != 1 || len(out.Models) != 1 || strings.Join(out.Models[0].Thinking, "|") != "low|high" {
		t.Fatalf("models = %+v", out)
	}
}

func TestChainMapKeepsOrderedStrings(t *testing.T) {
	got := chainMap([]byte(`{"default":["a:max",1,"b"],"web":"web/x","none":[]}`))
	want := map[string][]string{"default": {"a:max", "b"}, "web": {"web/x"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("chainMap = %v", got)
	}
}
