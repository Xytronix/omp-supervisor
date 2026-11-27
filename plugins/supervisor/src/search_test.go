package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

func TestNormalizeSessionID(t *testing.T) {
	cases := map[string]string{
		"": " ",
		"  01a033d1-abf2-7000-8824-20f09687ad0e  ": "01a033d1-abf2-7000-8824-20f09687ad0e",
		"omp:01a033d1-abf2-7000-8824-20f09687ad0e": "01a033d1-abf2-7000-8824-20f09687ad0e",
		"OMP:01a033d1-abf2-7000-8824-20f09687ad0e": "01a033d1-abf2-7000-8824-20f09687ad0e",
	}

	if got := normalizeSessionID(""); got != "" {
		t.Errorf("empty => %q", got)
	}
	if got := normalizeSessionID("  01a033d1-abf2-7000-8824-20f09687ad0e  "); got != "01a033d1-abf2-7000-8824-20f09687ad0e" {
		t.Errorf("trim => %q", got)
	}
	if got := normalizeSessionID("omp:01a033d1-abf2-7000-8824-20f09687ad0e"); got != "01a033d1-abf2-7000-8824-20f09687ad0e" {
		t.Errorf("prefix => %q", got)
	}
	if got := normalizeSessionID("OMP:01a033d1-abf2-7000-8824-20f09687ad0e"); got != "01a033d1-abf2-7000-8824-20f09687ad0e" {
		t.Errorf("prefix case => %q", got)
	}
	_ = cases
}

func TestParseOmpModelsJSONAndFilter(t *testing.T) {
	raw := []byte(`{"models":[{"provider":"cursor","id":"cursor-grok-4.6","selector":"cursor/cursor-grok-4.6","name":"Grok 4.6","contextWindow":200000,"maxTokens":64000,"thinking":["low","medium","high","xhigh"],"input":["text"]},{"provider":"google-antigravity","id":"gemini-3.7-flash","selector":"google-antigravity/gemini-3.7-flash","name":"Gemini 3.7 Flash","contextWindow":1000000,"maxTokens":66000,"thinking":["minimal","low","medium","high"],"input":["text","image"]}]}`)
	models := parseOmpModelsJSON(raw)
	if len(models) != 2 {
		t.Fatalf("expected 2 models, got %d", len(models))
	}
	if models[1].Selector != "google-antigravity/gemini-3.7-flash" {
		t.Errorf("selector = %q", models[1].Selector)
	}
	if !models[1].Images {
		t.Errorf("expected images true for gemini")
	}
	hits := filterModels(models, "grok", "", 10)
	if len(hits) != 1 || hits[0].ID != "cursor-grok-4.6" {
		t.Fatalf("filter grok => %+v", hits)
	}
	hits = filterModels(models, "", "google-antigravity", 10)
	if len(hits) != 1 || hits[0].ID != "gemini-3.7-flash" {
		t.Fatalf("filter provider => %+v", hits)
	}
	providers := aggregateProviders(models)
	if len(providers) != 2 {
		t.Fatalf("providers => %+v", providers)
	}
}

func TestHandleModelsUsesCatalog(t *testing.T) {
	setModelsCatalogForTest([]ompModel{
		{Provider: "cursor", ID: "cursor-grok-4.6", Selector: "cursor/cursor-grok-4.6", Name: "Grok 4.6"},
		{Provider: "google-antigravity", ID: "gemini-3.7-flash", Selector: "google-antigravity/gemini-3.7-flash", Name: "Gemini 3.7 Flash"},
	})
	req := mcp.CallToolRequest{}
	req.Params.Name = "models"
	req.Params.Arguments = map[string]any{"query": "grok"}
	res, err := handleModels(context.Background(), req)
	if err != nil {
		t.Fatalf("handleModels: %v", err)
	}
	if res.IsError {
		t.Fatalf("unexpected error result")
	}
	text := toolResultText(res)
	if !strings.Contains(text, "cursor/cursor-grok-4.6") {
		t.Fatalf("missing selector in %s", text)
	}
}

func TestLocalSessionSearchAndRead(t *testing.T) {
	tempHome, err := os.MkdirTemp("", "omp-session-search-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempHome)
	origHome := os.Getenv("HOME")
	defer os.Setenv("HOME", origHome)
	os.Setenv("HOME", tempHome)
	os.Setenv("OMP_SESSIONS_DIR", filepath.Join(tempHome, ".omp", "agent", "sessions"))
	defer os.Unsetenv("OMP_SESSIONS_DIR")

	cwd := "/tmp/test-project"
	slug := strings.ReplaceAll(cwd, "/", "-")
	dir := filepath.Join(tempHome, ".omp", "agent", "sessions", slug)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	sessionID := "01a033d1-abf2-7000-8824-20f09687ad0e"
	path := filepath.Join(dir, "2026-08-24T12-49-34-194Z_"+sessionID+".jsonl")
	body := strings.Join([]string{
		`{"type":"title","v":1,"title":"Investigate Bifrost and oh-my-pi configuration","updatedAt":"2026-08-24T12:58:35.290Z"}`,
		`{"type":"session","version":3,"id":"` + sessionID + `","timestamp":"2026-08-24T12:49:34.194Z","cwd":"` + cwd + `"}`,
		`{"type":"model_change","id":"m1","timestamp":"2026-08-24T12:49:34.200Z","model":"google-antigravity/gemini-3.7-flash","resolvedModelIsFallback":false}`,
		`{"type":"thinking_level_change","id":"t1","timestamp":"2026-08-24T12:49:34.201Z","thinkingLevel":"high"}`,
		`{"type":"message","id":"u1","timestamp":"2026-08-24T12:49:35Z","message":{"role":"user","content":[{"type":"text","text":"Look at bifrost OAuth2 and report the token path"}]}}`,
		`{"type":"message","id":"a1","timestamp":"2026-08-24T12:49:40Z","message":{"role":"assistant","content":[{"type":"text","text":"Checked the token path. Bearer sk-abcdefghijklmnopqrstuvwxyz0123456789 is redacted."}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	_ = os.Chtimes(path, now, now)

	hits := listLocalSessions("bifrost", "", 10)
	if len(hits) != 1 {
		t.Fatalf("expected 1 session, got %+v", hits)
	}
	if hits[0].SessionID != sessionID {
		t.Fatalf("session_id %q", hits[0].SessionID)
	}
	if hits[0].Title != "Investigate Bifrost and oh-my-pi configuration" {
		t.Fatalf("title %q", hits[0].Title)
	}

	sum, msgs, err := readSessionConversation("omp:"+sessionID, 10, 4000)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if sum.SessionID != sessionID {
		t.Fatalf("read id %q", sum.SessionID)
	}
	if len(msgs) != 2 {
		t.Fatalf("messages %+v", msgs)
	}
	if strings.Contains(msgs[1].Text, "sk-abcdefghijklmnopqrstuvwxyz0123456789") {
		t.Fatalf("secret leaked: %s", msgs[1].Text)
	}

	req := mcp.CallToolRequest{}
	req.Params.Name = "session"
	req.Params.Arguments = map[string]any{"session_id": "omp:" + sessionID}
	res, err := handleSession(context.Background(), req)
	if err != nil || res.IsError {
		t.Fatalf("handleSession err=%v res=%v", err, res)
	}
	if !strings.Contains(toolResultText(res), sessionID) {
		t.Fatalf("missing session id in %s", toolResultText(res))
	}

	reqID := mcp.CallToolRequest{}
	reqID.Params.Name = "session"
	reqID.Params.Arguments = map[string]any{"id": sessionID}
	resID, err := handleSession(context.Background(), reqID)
	if err != nil {
		t.Fatalf("handleSession id-only: %v", err)
	}
	if !resID.IsError {
		t.Fatalf("expected session to reject bare id alias, got %s", toolResultText(resID))
	}
}

func toolResultText(res *mcp.CallToolResult) string {
	if res == nil {
		return ""
	}
	b, _ := json.Marshal(res)
	return string(b)
}
