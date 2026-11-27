package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func TestSanitizeSecretString(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		{
			input:    "Bearer abc12345def67890==",
			expected: "Bearer [REDACTED]",
		},
		{
			input:    "key=sk-123456789012345678901234567890",
			expected: "key=[REDACTED]",
		},
		{
			input:    "Authorization: Bearer my-secret-token",
			expected: "Authorization: [REDACTED]",
		},
		{
			input:    "normal text without secrets",
			expected: "normal text without secrets",
		},
	}

	for _, c := range cases {
		got := sanitizeSecretString(c.input)
		if got != c.expected {
			t.Errorf("sanitizeSecretString(%q) = %q, expected %q", c.input, got, c.expected)
		}
	}
}

func TestExtractTitle(t *testing.T) {
	if got := extractTitle("# Investigate Bug in Auth"); got != "Investigate Bug in Auth" {
		t.Errorf("unexpected title: %q", got)
	}
	if got := extractTitle("Task: Fix memory leak in sidecar"); got != "Fix memory leak in sidecar" {
		t.Errorf("unexpected title: %q", got)
	}
}

func TestParseModelAndThinking(t *testing.T) {
	tests := []struct {
		name         string
		rawModel     string
		thinkingArg  string
		wantModel    string
		wantThinking string
		wantSplit    bool
	}{
		{
			name:         "default model with colon-thinking",
			rawModel:     "google-antigravity/gemini-3.7-flash:high",
			thinkingArg:  "",
			wantModel:    "google-antigravity/gemini-3.7-flash",
			wantThinking: "high",
			wantSplit:    true,
		},
		{
			name:         "default model with explicit thinking arg override",
			rawModel:     "google-antigravity/gemini-3.7-flash:high",
			thinkingArg:  "low",
			wantModel:    "google-antigravity/gemini-3.7-flash",
			wantThinking: "low",
			wantSplit:    true,
		},
		{
			name:         "exact model without thinking injects no level",
			rawModel:     "cursor/cursor-grok-4.6",
			thinkingArg:  "",
			wantModel:    "cursor/cursor-grok-4.6",
			wantThinking: "",
			wantSplit:    false,
		},
		{
			name:         "cursor grok with thinking suffix",
			rawModel:     "cursor/cursor-grok-4.6:max",
			thinkingArg:  "",
			wantModel:    "cursor/cursor-grok-4.6",
			wantThinking: "max",
			wantSplit:    true,
		},
		{
			name:         "colon in model that is NOT a thinking suffix stays intact",
			rawModel:     "my-custom-provider:port/model",
			thinkingArg:  "",
			wantModel:    "my-custom-provider:port/model",
			wantThinking: "",
			wantSplit:    false,
		},
		{
			name:         "ollama model with tag that is not thinking suffix",
			rawModel:     "ollama/llama3:8b",
			thinkingArg:  "",
			wantModel:    "ollama/llama3:8b",
			wantThinking: "",
			wantSplit:    false,
		},
		{
			name:         "id with a colon keeps its trailing effort suffix",
			rawModel:     "bifrost/ExperientalLabs/glm-5.3-uncensored:free:max",
			thinkingArg:  "",
			wantModel:    "bifrost/ExperientalLabs/glm-5.3-uncensored:free",
			wantThinking: "max",
			wantSplit:    true,
		},
		{
			name:         "id with a colon and no effort passes no level",
			rawModel:     "bifrost/ExperientalLabs/glm-5.3-uncensored:free",
			thinkingArg:  "",
			wantModel:    "bifrost/ExperientalLabs/glm-5.3-uncensored:free",
			wantThinking: "",
			wantSplit:    false,
		},
		{
			name:         "empty raw model leaves the destination default role in charge",
			rawModel:     "",
			thinkingArg:  "",
			wantModel:    "",
			wantThinking: "",
			wantSplit:    false,
		},
		{
			name:         "role selector keeps the destination role's effort",
			rawModel:     "@task",
			thinkingArg:  "",
			wantModel:    "@task",
			wantThinking: "",
			wantSplit:    false,
		},
		{
			name:         "role selector with effort suffix",
			rawModel:     "@slow:xhigh",
			thinkingArg:  "",
			wantModel:    "@slow",
			wantThinking: "xhigh",
			wantSplit:    true,
		},
		{
			name:         "all thinking levels: off, minimal, low, medium, high, xhigh, max, auto",
			rawModel:     "anthropic/claude-3-5-sonnet:xhigh",
			thinkingArg:  "",
			wantModel:    "anthropic/claude-3-5-sonnet",
			wantThinking: "xhigh",
			wantSplit:    true,
		},
		{
			name:         "thinking level auto",
			rawModel:     "openai/gpt-5:auto",
			thinkingArg:  "",
			wantModel:    "openai/gpt-5",
			wantThinking: "auto",
			wantSplit:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotModel, gotThinking, gotSplit := parseModelAndThinking(tt.rawModel, tt.thinkingArg)
			if gotModel != tt.wantModel {
				t.Errorf("parseModelAndThinking() gotModel = %v, want %v", gotModel, tt.wantModel)
			}
			if gotThinking != tt.wantThinking {
				t.Errorf("parseModelAndThinking() gotThinking = %v, want %v", gotThinking, tt.wantThinking)
			}
			if gotSplit != tt.wantSplit {
				t.Errorf("parseModelAndThinking() gotSplit = %v, want %v", gotSplit, tt.wantSplit)
			}
		})
	}
}

func TestShouldAllowHome(t *testing.T) {
	origHome := os.Getenv("HOME")
	defer os.Setenv("HOME", origHome)

	os.Setenv("HOME", "/home/ubuntu")

	if !shouldAllowHome("/home/ubuntu") {
		t.Errorf("expected /home/ubuntu to allow home")
	}
	if !shouldAllowHome("/home/ubuntu/") {
		t.Errorf("expected /home/ubuntu/ to allow home")
	}
	if shouldAllowHome("/tmp/omp-test") {
		t.Errorf("expected /tmp/omp-test to NOT allow home")
	}
}

func TestBuildOmpArgs(t *testing.T) {
	t.Run("basic new job", func(t *testing.T) {
		args := buildOmpArgs("Fix bug", "google-antigravity/gemini-3.7-flash", "high", "/tmp/project", false, "", "", false, false, false, "")
		expected := []string{
			"-p",
			"--model=google-antigravity/gemini-3.7-flash",
			"--thinking=high",
			"--cwd=/tmp/project",
			"Fix bug",
		}
		if len(args) != len(expected) {
			t.Fatalf("expected %d args, got %d: %v", len(expected), len(args), args)
		}
		for i, v := range expected {
			if args[i] != v {
				t.Errorf("arg[%d] = %q; expected %q", i, args[i], v)
			}
		}
	})

	t.Run("with allowHome, yolo, maxTime", func(t *testing.T) {
		args := buildOmpArgs("Refactor code", "cursor/cursor-grok-4.6", "medium", "/home/ubuntu", true, "10m", "", false, true, false, "")
		hasAllowHome := false
		hasAutoApprove := false
		hasMaxTime := false
		for _, a := range args {
			if a == "--allow-home" {
				hasAllowHome = true
			}
			if a == "--auto-approve" {
				hasAutoApprove = true
			}
			if a == "--max-time=10m" {
				hasMaxTime = true
			}
		}
		if !hasAllowHome {
			t.Errorf("expected --allow-home in args: %v", args)
		}
		if !hasAutoApprove {
			t.Errorf("expected --auto-approve in args: %v", args)
		}
		if !hasMaxTime {
			t.Errorf("expected --max-time=10m in args: %v", args)
		}
	})

	t.Run("resume with sessionID", func(t *testing.T) {
		sessID := "01a037dd-8b65-713d-b297-2eb4954fc631"
		args := buildOmpArgs("Follow up prompt", "google-antigravity/gemini-3.7-flash", "high", "/tmp/project", false, "", sessID, false, false, false, "")
		hasR := false
		hasC := false
		for i, a := range args {
			if a == "-r" && i+1 < len(args) && args[i+1] == sessID {
				hasR = true
			}
			if a == "-c" {
				hasC = true
			}
		}
		if !hasR {
			t.Errorf("expected -r %s in args: %v", sessID, args)
		}
		if hasC {
			t.Errorf("did not expect -c in args when sessionID is set: %v", args)
		}
	})

	t.Run("continue previous session", func(t *testing.T) {
		args := buildOmpArgs("Follow up prompt", "google-antigravity/gemini-3.7-flash", "high", "/tmp/project", false, "", "", true, false, false, "")
		hasR := false
		hasC := false
		for _, a := range args {
			if a == "-r" {
				hasR = true
			}
			if a == "-c" {
				hasC = true
			}
		}
		if !hasC {
			t.Errorf("expected -c in args: %v", args)
		}
		if hasR {
			t.Errorf("did not expect -r in args: %v", args)
		}
	})

	t.Run("sessionID takes precedence over continue", func(t *testing.T) {
		sessID := "01a037dd-8b65-713d-b297-2eb4954fc631"
		args := buildOmpArgs("Follow up prompt", "google-antigravity/gemini-3.7-flash", "high", "/tmp/project", false, "", sessID, true, false, false, "")
		hasR := false
		hasC := false
		for i, a := range args {
			if a == "-r" && i+1 < len(args) && args[i+1] == sessID {
				hasR = true
			}
			if a == "-c" {
				hasC = true
			}
		}
		if !hasR {
			t.Errorf("expected -r %s in args: %v", sessID, args)
		}
		if hasC {
			t.Errorf("did NOT expect -c in args when sessionID is present: %v", args)
		}
	})

	t.Run("steerable job runs rpc mode without an argv prompt", func(t *testing.T) {
		sessID := "01a037dd-8b65-713d-b297-2eb4954fc631"
		args := buildOmpArgs("Investigate flake", "google-antigravity/gemini-3.7-flash", "high", "/tmp/project", true, "10m", sessID, false, false, true, "")
		if args[0] != "--mode=rpc" {
			t.Fatalf("expected --mode=rpc first, got %v", args)
		}
		for _, a := range args {
			if a == "-p" {
				t.Errorf("steerable job must not use print mode: %v", args)
			}
			if a == "Investigate flake" {
				t.Errorf("prompt must be sent as an RPC command, not argv: %v", args)
			}
		}
		if args[len(args)-2] != "-r" || args[len(args)-1] != sessID {
			t.Errorf("expected resume flags preserved in rpc mode: %v", args)
		}
	})
}

func TestSessionFileParsing(t *testing.T) {
	tempHome, err := os.MkdirTemp("", "omp-session-test-home-*")
	if err != nil {
		t.Fatalf("failed to create temp home: %v", err)
	}
	defer os.RemoveAll(tempHome)

	origHome := os.Getenv("HOME")
	defer os.Setenv("HOME", origHome)
	os.Setenv("HOME", tempHome)

	targetCWD := "/tmp/test-project"
	cwdSlug := strings.ReplaceAll(targetCWD, "/", "-")
	sessDir := filepath.Join(tempHome, ".omp", "agent", "sessions", cwdSlug)
	if err := os.MkdirAll(sessDir, 0755); err != nil {
		t.Fatalf("failed to create session dir: %v", err)
	}

	sessionID := "01a037dd-8b65-713d-b297-2eb4954fc631"
	sessFileName := fmt.Sprintf("2026-08-25T07-41-01-157Z_%s.jsonl", sessionID)
	sessFilePath := filepath.Join(sessDir, sessFileName)

	jsonlLines := []string{
		`{"type":"session","version":3,"id":"01a037dd-8b65-713d-b297-2eb4954fc631","timestamp":"2026-08-25T07:41:01.157Z","cwd":"/tmp/test-project"}`,
		`{"type":"model_change","id":"02c00365","parentId":null,"timestamp":"2026-08-25T07:41:01.899Z","model":"google-antigravity/gemini-3.7-flash","resolvedModelIsFallback":false}`,
		`{"type":"thinking_level_change","id":"6fb0d849","parentId":"02c00365","timestamp":"2026-08-25T07:41:01.899Z","thinkingLevel":"high","configured":null}`,
		`{"type":"custom","customType":"tool_execution_start","data":{"toolCallId":"call_1","toolName":"write","args":{"path":"pkg/main.go"}}}`,
		`{"type":"message","message":{"role":"toolResult","toolName":"todo","content":[{"type":"text","text":"Overall: 4/6 done, 2 open"}]}}`,
	}

	var buf bytes.Buffer
	for _, l := range jsonlLines {
		buf.WriteString(l + "\n")
	}
	if err := os.WriteFile(sessFilePath, buf.Bytes(), 0644); err != nil {
		t.Fatalf("failed to write session file: %v", err)
	}

	job := &Job{
		ID:             "job_sess_test",
		PID:            12345,
		SessionID:      sessionID,
		Model:          "google-antigravity/gemini-3.7-flash:high",
		ModelRequested: "google-antigravity/gemini-3.7-flash:high",
		CWD:            targetCWD,
		StartTime:      time.Now().Add(-1 * time.Minute),
	}

	job.refreshSessionMetadata()

	if job.SessionID != sessionID {
		t.Errorf("expected session_id %q, got %q", sessionID, job.SessionID)
	}
	if job.ModelResolved != "google-antigravity/gemini-3.7-flash" {
		t.Errorf("expected model_resolved 'google-antigravity/gemini-3.7-flash', got %q", job.ModelResolved)
	}
	if job.Fallback != false {
		t.Errorf("expected fallback false, got true")
	}
	if job.Thinking != "high" {
		t.Errorf("expected thinking 'high', got %q", job.Thinking)
	}
	if job.LastTool != "write" {
		t.Errorf("expected last_tool 'write', got %q", job.LastTool)
	}
	if job.TodosDone != 4 || job.TodosOpen != 2 {
		t.Errorf("expected todos 4 done, 2 open; got %d done, %d open", job.TodosDone, job.TodosOpen)
	}
	hasFile := false
	for _, f := range job.FilesChanged {
		if f == "pkg/main.go" {
			hasFile = true
			break
		}
	}
	if !hasFile {
		t.Errorf("expected pkg/main.go in files_changed: %v", job.FilesChanged)
	}
}

func TestPrintModeSessionSwitchesNameTheReplacedModel(t *testing.T) {
	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)
	targetCWD := "/tmp/test-print-switch"
	sessDir := filepath.Join(tempHome, ".omp", "agent", "sessions", strings.ReplaceAll(targetCWD, "/", "-"))
	if err := os.MkdirAll(sessDir, 0755); err != nil {
		t.Fatal(err)
	}
	sessionID := "01a1df40-0000-7000-8000-000000000001"
	lines := strings.Join([]string{
		`{"type":"session","version":3,"id":"` + sessionID + `","timestamp":"2026-11-15T12:48:21.000Z","cwd":"` + targetCWD + `"}`,
		`{"type":"model_change","id":"a1","parentId":null,"timestamp":"2026-11-15T12:48:21.100Z","model":"bifrost/anthropic/claude-haiku-4-5-20251001","resolvedModelIsFallback":false}`,
		`{"type":"model_change","id":"a2","parentId":"a1","timestamp":"2026-11-15T12:48:24.000Z","model":"bifrost/omp-gw/factory-droid/deepseek-v4.1-flash-1017","resolvedModelIsFallback":true}`,
		`{"type":"model_switch","id":"a3","parentId":"a2","timestamp":"2026-11-15T12:48:24.000Z","from":"bifrost/anthropic/claude-haiku-4-5-20251001","to":"bifrost/omp-gw/factory-droid/deepseek-v4.1-flash-1017","effortTo":"max","reason":"Request failed: 400 credit balance too low","kind":"retry"}`,
	}, "\n") + "\n"
	if err := os.WriteFile(filepath.Join(sessDir, "2026-11-15T12-48-21-000Z_"+sessionID+".jsonl"), []byte(lines), 0644); err != nil {
		t.Fatal(err)
	}
	job := &Job{
		ID:             "job_print_switch",
		SessionID:      sessionID,
		ModelRequested: "bifrost/anthropic/claude-haiku-4-5-20251001",
		CWD:            targetCWD,
		StartTime:      time.Now().Add(-time.Minute),
	}
	for range 2 {
		job.refreshSessionMetadata()
	}
	if len(job.ModelSwitches) != 1 || job.FallbackFrom != "bifrost/anthropic/claude-haiku-4-5-20251001" || !job.Fallback {
		t.Fatalf("switches=%+v fallback=%v from=%q", job.ModelSwitches, job.Fallback, job.FallbackFrom)
	}
	want := `fallback="bifrost/anthropic/claude-haiku-4-5-20251001 -&gt; bifrost/omp-gw/factory-droid/deepseek-v4.1-flash-1017: Request failed: 400 credit balance too low"`
	if line := job.modelLineLocked(); !strings.Contains(line, want) {
		t.Fatalf("model line %s lacks %s", line, want)
	}
}

func TestFindSessionFileHomeRelativeDirs(t *testing.T) {
	tempHome := t.TempDir()
	base := filepath.Join(tempHome, ".omp", "agent", "sessions")
	start := time.Now().Add(-time.Minute)

	write := func(dir, name string) string {
		if err := os.MkdirAll(filepath.Join(base, dir), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		path := filepath.Join(base, dir, name)
		if err := os.WriteFile(path, []byte("{}\n"), 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		return path
	}

	homeSession := write("-", "2026-09-06T04-28-29-968Z_01a074f9-9990-7000-af48-ca4ae705bc62.jsonl")
	repoSession := write("-mcp-sidecars-supervisor", "2026-09-06T04-46-07-404Z_01a07509-bc2c-7000-acdc-52540ff98009.jsonl")
	outsideSession := write("-tmp-oh-my-pi", "2026-09-06T04-50-00-000Z_01a07510-1111-7000-8000-52540ff98010.jsonl")

	cases := []struct {
		name string
		cwd  string
		want string
	}{
		{"home itself", tempHome, homeSession},
		{"under home", filepath.Join(tempHome, "mcp-sidecars", "supervisor"), repoSession},
		{"outside home", "/tmp/oh-my-pi", outsideSession},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := findSessionFile(tempHome, tc.cwd, "", start); got != tc.want {
				t.Errorf("timeframe fallback: got %q, want %q", got, tc.want)
			}
			sessionID := strings.TrimSuffix(filepath.Base(tc.want), ".jsonl")
			sessionID = sessionID[strings.LastIndex(sessionID, "_")+1:]
			if got := findSessionFile(tempHome, tc.cwd, sessionID, start); got != tc.want {
				t.Errorf("exact id lookup: got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestJobStoreAndTools(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "supervisor-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store, err := newJobStore(tempDir)
	if err != nil {
		t.Fatalf("failed to create job store: %v", err)
	}
	globalStore = store

	mcpServer := server.NewMCPServer("Supervisor", "1.0.0")
	registerTools(mcpServer)

	streamableServer := server.NewStreamableHTTPServer(mcpServer, server.WithStateLess(true))
	mux := http.NewServeMux()
	mux.Handle("/mcp", streamableServer)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	initReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2026-03-26",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]any{
				"name":    "test-client",
				"version": "1.0",
			},
		},
	}
	initBody, _ := json.Marshal(initReq)
	req, _ := http.NewRequest("POST", ts.URL+"/mcp", bytes.NewReader(initBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	initResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("MCP initialize failed: %v", err)
	}
	if initResp.StatusCode != http.StatusOK {
		t.Errorf("MCP initialize returned status %d", initResp.StatusCode)
	}

	for _, toolName := range []string{"status", "result", "cancel"} {
		toolReq := map[string]any{
			"jsonrpc": "2.0",
			"id":      10,
			"method":  "tools/call",
			"params": map[string]any{
				"name": toolName,
				"arguments": map[string]any{
					"id": "nonexistent_id",
				},
			},
		}
		toolBody, _ := json.Marshal(toolReq)
		r, _ := http.NewRequest("POST", ts.URL+"/mcp", bytes.NewReader(toolBody))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", "application/json, text/event-stream")
		res, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatalf("tools/call %s failed: %v", toolName, err)
		}

		var callResult struct {
			Result struct {
				IsError bool `json:"isError"`
			} `json:"result"`
		}
		json.NewDecoder(res.Body).Decode(&callResult)
		if !callResult.Result.IsError {
			t.Errorf("expected isError: true for nonexistent job on tool %s", toolName)
		}
	}

	dummyJob := &Job{
		ID:             "sub_test_123",
		PID:            99999,
		SessionID:      "01a037dd-8b65-713d-b297-2eb4954fc631",
		Model:          "google-antigravity/gemini-3.7-flash:high",
		ModelRequested: "google-antigravity/gemini-3.7-flash:high",
		ModelResolved:  "google-antigravity/gemini-3.7-flash",
		Fallback:       false,
		Thinking:       "high",
		Prompt:         "Smoke test prompt",
		Title:          "Smoke test prompt",
		CWD:            tempDir,
		Status:         "done",
		ExitCode:       0,
		StartTime:      time.Now().Add(-10 * time.Second),
		EndTime:        time.Now(),
		LastTool:       "read",
		TodosDone:      2,
		TodosOpen:      0,
		FilesChanged: []string{
			"REPORT.md",
		},
	}

	reportPath := filepath.Join(tempDir, "REPORT.md")
	os.WriteFile(reportPath, []byte("# Smoke Test Report\n\nAll tasks completed successfully with Bearer secret-token-12345."), 0644)

	store.add(dummyJob)

	statusReq2 := map[string]any{
		"jsonrpc": "2.0",
		"id":      4,
		"method":  "tools/call",
		"params": map[string]any{
			"name": "status",
			"arguments": map[string]any{
				"id": "sub_test_123",
			},
		},
	}
	statusBody2, _ := json.Marshal(statusReq2)
	req4, _ := http.NewRequest("POST", ts.URL+"/mcp", bytes.NewReader(statusBody2))
	req4.Header.Set("Content-Type", "application/json")
	req4.Header.Set("Accept", "application/json, text/event-stream")
	statusResp2, _ := http.DefaultClient.Do(req4)

	var statOutput struct {
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	json.NewDecoder(statusResp2.Body).Decode(&statOutput)
	if statOutput.Result.IsError {
		t.Fatalf("unexpected error on valid status call")
	}

	var statObj map[string]any
	json.Unmarshal([]byte(statOutput.Result.Content[0].Text), &statObj)
	if statObj["status"] != "done" || statObj["id"] != "sub_test_123" {
		t.Errorf("unexpected status response: %v", statObj)
	}
	if statObj["job_id"] != "sub_test_123" {
		t.Errorf("expected job_id in status: %v", statObj["job_id"])
	}
	if statObj["model_requested"] != "google-antigravity/gemini-3.7-flash:high" {
		t.Errorf("unexpected model_requested in status: %v", statObj["model_requested"])
	}
	if statObj["model_resolved"] != "google-antigravity/gemini-3.7-flash" {
		t.Errorf("unexpected model_resolved in status: %v", statObj["model_resolved"])
	}
	if statObj["session_id"] != "01a037dd-8b65-713d-b297-2eb4954fc631" {
		t.Errorf("unexpected session_id in status: %v", statObj["session_id"])
	}

	resultReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      5,
		"method":  "tools/call",
		"params": map[string]any{
			"name": "result",
			"arguments": map[string]any{
				"id": "sub_test_123",
			},
		},
	}
	resultBody, _ := json.Marshal(resultReq)
	req5, _ := http.NewRequest("POST", ts.URL+"/mcp", bytes.NewReader(resultBody))
	req5.Header.Set("Content-Type", "application/json")
	req5.Header.Set("Accept", "application/json, text/event-stream")
	resResp, _ := http.DefaultClient.Do(req5)

	var resOutput struct {
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	json.NewDecoder(resResp.Body).Decode(&resOutput)
	if resOutput.Result.IsError {
		t.Fatalf("unexpected error on valid result call")
	}

	var resObj map[string]any
	json.Unmarshal([]byte(resOutput.Result.Content[0].Text), &resObj)
	if resObj["status"] != "done" || resObj["exit_code"].(float64) != 0 {
		t.Errorf("unexpected result response: %v", resObj)
	}
	if resObj["job_id"] != "sub_test_123" || resObj["id"] != "sub_test_123" {
		t.Errorf("expected job_id/id in result: %v", resObj)
	}
	if resObj["model_requested"] != "google-antigravity/gemini-3.7-flash:high" {
		t.Errorf("unexpected model_requested in result: %v", resObj["model_requested"])
	}
	if resObj["model_resolved"] != "google-antigravity/gemini-3.7-flash" {
		t.Errorf("unexpected model_resolved in result: %v", resObj["model_resolved"])
	}
	reportStr := resObj["report"].(string)
	if !bytes.Contains([]byte(reportStr), []byte("Bearer [REDACTED]")) {
		t.Errorf("expected secret to be redacted in report, got: %s", reportStr)
	}

	cancelReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      6,
		"method":  "tools/call",
		"params": map[string]any{
			"name": "cancel",
			"arguments": map[string]any{
				"id": "sub_test_123",
			},
		},
	}
	cancelBody, _ := json.Marshal(cancelReq)
	req6, _ := http.NewRequest("POST", ts.URL+"/mcp", bytes.NewReader(cancelBody))
	req6.Header.Set("Content-Type", "application/json")
	req6.Header.Set("Accept", "application/json, text/event-stream")
	cancelResp, _ := http.DefaultClient.Do(req6)

	var cancelOutput struct {
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	json.NewDecoder(cancelResp.Body).Decode(&cancelOutput)
	if cancelOutput.Result.IsError {
		t.Fatalf("unexpected error on cancel call for completed job")
	}
	var cancelObj map[string]any
	json.Unmarshal([]byte(cancelOutput.Result.Content[0].Text), &cancelObj)
	if cancelObj["canceled"] != false {
		t.Errorf("expected canceled: false for completed job, got %v", cancelObj["canceled"])
	}
	if cancelObj["status"] != "done" {
		t.Errorf("expected status: 'done', got %v", cancelObj["status"])
	}
	if cancelObj["job_id"] != "sub_test_123" {
		t.Errorf("expected job_id in cancel: %v", cancelObj["job_id"])
	}
}

func TestDirectToolHandlers(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "supervisor-direct-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store, err := newJobStore(tempDir)
	if err != nil {
		t.Fatalf("failed to create job store: %v", err)
	}
	globalStore = store

	req := mcp.CallToolRequest{}
	req.Params.Name = "run"
	req.Params.Arguments = map[string]any{
		"prompt": "",
	}
	res, err := handleRun(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res.IsError {
		t.Errorf("expected error for empty prompt")
	}

	reqStatus := mcp.CallToolRequest{}
	reqStatus.Params.Name = "status"
	reqStatus.Params.Arguments = map[string]any{
		"id": "",
	}
	resStatus, err := handleStatus(context.Background(), reqStatus)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resStatus.IsError {
		t.Errorf("expected error for empty id in status")
	}

	reqResult := mcp.CallToolRequest{}
	reqResult.Params.Name = "result"
	reqResult.Params.Arguments = map[string]any{
		"id": "",
	}
	resResult, err := handleResult(context.Background(), reqResult)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resResult.IsError {
		t.Errorf("expected error for empty id in result")
	}

	reqCancel := mcp.CallToolRequest{}
	reqCancel.Params.Name = "cancel"
	reqCancel.Params.Arguments = map[string]any{
		"id": "",
	}
	resCancel, err := handleCancel(context.Background(), reqCancel)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resCancel.IsError {
		t.Errorf("expected error for empty id in cancel")
	}

	dummy := &Job{
		ID:        "sub_alias_preferred",
		Status:    "done",
		SessionID: "01a033d1-abf2-7000-8824-20f09687ad0e",
		StartTime: time.Now().Add(-time.Second),
		EndTime:   time.Now(),
	}
	store.add(dummy)
	reqJobID := mcp.CallToolRequest{}
	reqJobID.Params.Name = "status"
	reqJobID.Params.Arguments = map[string]any{
		"job_id": "sub_alias_preferred",
		"id":     "nonexistent_id",
	}
	resJobID, err := handleStatus(context.Background(), reqJobID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resJobID.IsError {
		t.Fatalf("expected job_id to win over deprecated id")
	}
	var aliasObj map[string]any
	if err := json.Unmarshal([]byte(resJobID.Content[0].(mcp.TextContent).Text), &aliasObj); err != nil {
		t.Fatalf("parse status: %v", err)
	}
	if aliasObj["job_id"] != "sub_alias_preferred" {
		t.Errorf("job_id alias lookup failed: %v", aliasObj)
	}
}

func TestActiveProcessCancellation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "supervisor-cancel-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store, err := newJobStore(tempDir)
	if err != nil {
		t.Fatalf("failed to create job store: %v", err)
	}
	globalStore = store

	execCtx, cancelFn := context.WithCancel(context.Background())
	cmd := exec.CommandContext(execCtx, "sleep", "60")
	configureChildProcess(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start mock sleep process: %v", err)
	}
	go func() { _ = cmd.Wait() }()
	pid := cmd.Process.Pid

	job := &Job{
		ID:        "sub_cancel_active_test",
		PID:       pid,
		SessionID: "01a037dd-8b65-713d-b297-2eb4954fc631",
		Status:    "running",
		StartTime: time.Now(),
		cmd:       cmd,
		cancelFn:  cancelFn,
	}
	store.add(job)

	reqCancel := mcp.CallToolRequest{}
	reqCancel.Params.Name = "cancel"
	reqCancel.Params.Arguments = map[string]any{
		"id": "sub_cancel_active_test",
	}
	res, err := handleCancel(context.Background(), reqCancel)
	if err != nil {
		t.Fatalf("handleCancel returned error: %v", err)
	}
	if res.IsError {
		t.Fatalf("expected success, got error result")
	}

	var resObj map[string]any
	if err := json.Unmarshal([]byte(res.Content[0].(mcp.TextContent).Text), &resObj); err != nil {
		t.Fatalf("failed to parse cancel response: %v", err)
	}
	if resObj["canceled"] != true {
		t.Errorf("expected canceled: true, got %v", resObj["canceled"])
	}
	if resObj["status"] != "canceled" {
		t.Errorf("expected status: 'canceled', got %v", resObj["status"])
	}
	if resObj["job_id"] != "sub_cancel_active_test" {
		t.Errorf("expected job_id in active cancel: %v", resObj["job_id"])
	}
	if isProcessAlive(pid) {
		t.Errorf("expected process %d to be dead after cancel", pid)
	}
}

func TestGetReportHeadCutsMarkdownAndFinalText(t *testing.T) {
	dir := t.TempDir()
	body := "HEAD-START-" + strings.Repeat("A", 200) + "-TAIL-END"
	if err := os.WriteFile(filepath.Join(dir, "REPORT.md"), []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	job := &Job{CWD: dir, Status: "done"}
	got, trunc := job.getReport(20)
	if !trunc {
		t.Fatal("expected REPORT.md to be truncated")
	}
	if !strings.HasPrefix(got, "HEAD-START-") {
		t.Fatalf("REPORT.md should be head-cut, got %q", got)
	}
	if strings.Contains(got, "TAIL-END") {
		t.Fatalf("REPORT.md should not keep the tail, got %q", got)
	}

	empty := t.TempDir()
	job = &Job{CWD: empty, Report: "BEGIN-" + strings.Repeat("x", 200) + "-END", Status: "done"}
	got, trunc = job.getReport(20)
	if !trunc {
		t.Fatal("expected final assistant text to be truncated")
	}
	if !strings.HasPrefix(got, "BEGIN-") {
		t.Fatalf("final text should be head-cut, got %q", got)
	}
	if strings.Contains(got, "-END") {
		t.Fatalf("final text should not keep the tail, got %q", got)
	}
}

func TestGetReportTailCutsLogs(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "job.log")
	body := "HEAD-START-" + strings.Repeat("Z", 200) + "-TAIL-END"
	if err := os.WriteFile(logPath, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	job := &Job{CWD: dir, LogPath: logPath, Status: "done"}
	got, trunc := job.getReport(20)
	if !trunc {
		t.Fatal("expected log to be truncated")
	}
	if strings.Contains(got, "HEAD-START-") {
		t.Fatalf("log should be tail-cut, got %q", got)
	}
	if !strings.Contains(got, "TAIL-END") {
		t.Fatalf("log should keep the end, got %q", got)
	}
}

func TestHeadCutAndTailCut(t *testing.T) {
	s := "abcdef"
	got, trunc := headCut(s, 3)
	if got != "abc" || !trunc {
		t.Fatalf("headCut=%q trunc=%v", got, trunc)
	}
	got, trunc = tailCut(s, 3)
	if got != "def" || !trunc {
		t.Fatalf("tailCut=%q trunc=%v", got, trunc)
	}
	got, trunc = headCut(s, 10)
	if got != s || trunc {
		t.Fatalf("short headCut=%q trunc=%v", got, trunc)
	}
}

func TestLocateOmpBinaryHonorsOMPBIN(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "omp")
	if err := os.WriteFile(bin, []byte("#!/bin/true\n"), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OMP_BIN", bin)
	t.Setenv("OMP_PATH", "")
	got, err := locateOmpBinary()
	if err != nil {
		t.Fatalf("locateOmpBinary: %v", err)
	}
	if got != bin {
		t.Fatalf("got %q want %q", got, bin)
	}
}

func TestResolveJobCwd(t *testing.T) {
	dir := t.TempDir()
	got, err := resolveJobCwd(dir)
	if err != nil {
		t.Fatalf("resolveJobCwd existing: %v", err)
	}
	if got != dir {
		t.Fatalf("got %q want %q", got, dir)
	}
	missing := filepath.Join(dir, "nope")
	_, err = resolveJobCwd(missing)
	if err == nil {
		t.Fatal("expected error for missing cwd")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("error should name missing cwd, got %v", err)
	}
	t.Setenv("HOME", dir)
	got, err = resolveJobCwd("")
	if err != nil {
		t.Fatalf("empty cwd: %v", err)
	}
	if got != dir {
		t.Fatalf("empty cwd got %q want HOME %q", got, dir)
	}
}
