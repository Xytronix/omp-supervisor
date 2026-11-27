package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

func newTestStack(t *testing.T) *httptest.Server {
	t.Helper()
	newTestStores(t)
	t.Setenv("SUPERVISOR_REGISTRY_TOKEN", testEnrollToken)
	registry, err := newAgentRegistry(t.TempDir())
	if err != nil {
		t.Fatalf("newAgentRegistry: %v", err)
	}
	globalAgents = registry
	srv := httptest.NewServer(registry.registryMux())
	t.Cleanup(srv.Close)
	return srv
}

func agentRows(t *testing.T, payload map[string]any) []map[string]any {
	t.Helper()
	raw, ok := payload["agents"]
	if !ok {
		t.Fatalf("response has no agents field: %v", payload)
	}
	items, _ := raw.([]any)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		row, _ := item.(map[string]any)
		out = append(out, row)
	}
	return out
}

func TestAgentsListsRegisteredSessionsAndLocalJobs(t *testing.T) {
	t.Setenv("SUPERVISOR_MACHINE", "test-local-host")
	srv := newTestStack(t)
	registerTestAgent(t, srv, map[string]any{
		"machine":    "mac-mini",
		"host":       map[string]any{"hostname": "devbook.local", "platform": "darwin", "architecture": "arm64"},
		"name":       "mac-mini:project",
		"session_id": "sess-mac",
		"cwd":        "/Users/dev/code/project",
		"model":      "openai-codex/gpt-5.6",
		"title":      "refactor auth",
		"harness":    "omp-tty",
	})
	globalStore.add(&Job{
		ID:        "sub_local",
		PID:       os.Getpid(),
		Status:    "running",
		Steerable: true,
		SessionID: "sess-local",
		CWD:       "/home/ubuntu/repo",
		Model:     "google-antigravity/gemini-3.7-flash",
		Title:     "local child",
		StartTime: time.Now(),
	})

	globalStore.add(&Job{ID: "sub_print", PID: os.Getpid(), Status: "running", StartTime: time.Now()})

	_, payload := callTool(t, handleAgents, map[string]any{})
	rows := agentRows(t, payload)
	if len(rows) != 2 {
		t.Fatalf("expected 2 live agents, got %d: %v", len(rows), payload)
	}

	bySource := map[string]map[string]any{}
	for _, row := range rows {
		bySource[row["source"].(string)] = row
	}
	registered, ok := bySource["registered"]
	if !ok {
		t.Fatalf("registered agent missing: %v", rows)
	}
	if registered["machine"] != "mac-mini" || registered["session_id"] != "sess-mac" || registered["transport"] != "poll" {
		t.Fatalf("unexpected registered row: %v", registered)
	}
	host, ok := registered["host"].(map[string]any)
	if !ok || host["hostname"] != "devbook.local" || host["platform"] != "darwin" || host["architecture"] != "arm64" {
		t.Fatalf("registered host identity missing from live sessions: %v", registered)
	}
	job, ok := bySource["job"]
	if !ok {
		t.Fatalf("local job missing: %v", rows)
	}
	if job["agent_id"] != "sub_local" || job["transport"] != "rpc" {
		t.Fatalf("unexpected job row: %v", job)
	}

	_, filtered := callTool(t, handleAgents, map[string]any{"query": "mac-mini"})
	if rows := agentRows(t, filtered); len(rows) != 1 || rows[0]["session_id"] != "sess-mac" {
		t.Fatalf("query filter returned %v", rows)
	}
	_, byMachine := callTool(t, handleAgents, map[string]any{"machine": "test-local-host"})
	if rows := agentRows(t, byMachine); len(rows) != 1 || rows[0]["agent_id"] != "sub_local" {
		t.Fatalf("machine filter returned %v", rows)
	}
	_, byHostname := callTool(t, handleAgents, map[string]any{"query": "devbook.local"})
	if rows := agentRows(t, byHostname); len(rows) != 1 || rows[0]["machine"] != "mac-mini" {
		t.Fatalf("hostname discovery lost execution alias: %v", rows)
	}
	_, notAnAlias := callTool(t, handleAgents, map[string]any{"machine": "devbook.local"})
	if rows := agentRows(t, notAnAlias); len(rows) != 0 {
		t.Fatalf("machine filter silently adopted a hostname as launch alias: %v", rows)
	}
}

func TestRegisteredAsideRequiresExplicitCapabilityBeforeQueueing(t *testing.T) {
	for _, tc := range []struct {
		name         string
		capabilities []string
		supported    bool
	}{
		{name: "legacy omitted capabilities"},
		{name: "legacy bridge", capabilities: []string{"prompt", "steer", "follow_up"}},
		{name: "capability is exact", capabilities: []string{"prompt", "ASIDE"}},
		{name: "updated bridge", capabilities: []string{"prompt", "steer", "aside", "follow_up"}, supported: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestStack(t)
			id, token := registerTestAgent(t, srv, map[string]any{
				"machine": "test-host", "capabilities": tc.capabilities,
			})
			source := map[string]any{"kind": "agent", "from": "Peer", "name": "Researcher", "route": "omp"}
			for _, handler := range []func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error){handlePrompt, handleSteer} {
				result, payload := callTool(t, handler, map[string]any{
					"agent_id": id, "text": "peer input", "mode": "aside",
					"source": source, "wait_seconds": 0,
				})
				if tc.supported {
					if result.IsError || payload["status"] != "queued" || payload["mode"] != "aside" {
						t.Fatalf("updated bridge did not receive explicit aside: %v", payload)
					}
				} else {
					errText, _ := payload["__error_text"].(string)
					if !result.IsError || !strings.Contains(errText, "aside capability") || !strings.Contains(errText, "restart") {
						t.Fatalf("legacy bridge did not receive actionable capability rejection: %v", payload)
					}
				}
			}
			wantQueued := 0
			if tc.supported {
				wantQueued = 2
			}
			ag := globalAgents.get(id)
			if ag == nil || len(ag.Queue) != wantQueued || len(ag.History) != 0 {
				t.Fatalf("rejected aside mutated queue/history or supported aside was lost: %#v", ag)
			}
			code, inbox := doJSON(t, http.MethodGet, srv.URL+"/api/agents/"+id+"/inbox?wait=0", token, nil)
			deliveries, _ := inbox["deliveries"].([]any)
			if code != http.StatusOK || len(deliveries) != wantQueued {
				t.Fatalf("capability admission did not reach the actual pull queue: %v", inbox)
			}
			for _, raw := range deliveries {
				delivery := raw.(map[string]any)
				peer := delivery["source"].(map[string]any)
				if delivery["mode"] != "aside" || peer["from"] != "Peer" || peer["name"] != "Researcher" {
					t.Fatalf("supported aside was reinterpreted or lost provenance: %v", delivery)
				}
			}
			if !tc.supported {
				result, payload := callTool(t, handlePrompt, map[string]any{
					"agent_id": id, "text": "explicit follow-up", "mode": "follow_up", "wait_seconds": 0,
				})
				if result.IsError || payload["mode"] != "follow_up" || payload["status"] != "queued" {
					t.Fatalf("new-mode admission changed genuine legacy follow-up behavior: %v", payload)
				}
			}
		})
	}
}

func TestAgentsRequiresExplicitGrokDiscoveryAndKeepsOMPDefault(t *testing.T) {
	t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", sixGrokTargets)
	srv := newTestStack(t)
	id, _ := registerTestAgent(t, srv, map[string]any{"name": "coding", "machine": "omp-host"})
	result, payload := callTool(t, handleAgents, nil)
	if result.IsError {
		t.Fatal(payload)
	}
	if rows := agentRows(t, payload); len(rows) != 1 || rows[0]["agent_id"] != id || rows[0]["source"] != "registered" {
		t.Fatalf("ordinary discovery changed from OMP: %v", payload)
	}
	result, payload = callTool(t, handleAgents, map[string]any{"include_grok": true})
	if result.IsError {
		t.Fatal(payload)
	}
	rows := agentRows(t, payload)
	if len(rows) != 7 {
		t.Fatalf("explicit discovery lost OMP or one of six peers: %v", rows)
	}
	seen := make(map[string]bool, 6)
	for _, row := range rows {
		if row["source"] != "grok" {
			continue
		}
		peerID, _ := row["agent_id"].(string)
		if seen[peerID] || !isGrokPeerID(peerID) || row["status"] != "configured" || row["steerable"] != false {
			t.Fatalf("duplicate or fabricated webhook session: %v", row)
		}
		seen[peerID] = true
	}
	if len(seen) != 6 {
		t.Fatalf("expected six unique explicit peers: %v", seen)
	}
	result, payload = callTool(t, handlePrompt, map[string]any{"agent_id": "coding", "text": "ordinary OMP peer", "wait_seconds": 0})
	if result.IsError || payload["agent_id"] != id || payload["transport"] != "poll" {
		t.Fatalf("OMP name unexpectedly selected a webhook: %v", payload)
	}
	result, payload = callTool(t, handleAgent, map[string]any{"agent_id": "grok-coding"})
	if result.IsError || payload["agent"].(map[string]any)["status"] != "configured" {
		t.Fatalf("explicit webhook detail invented a running peer: %v", payload)
	}

	t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", `[{"id":"grok-coding","url":"one","key":"test"},{"id":"grok-coding","url":"two","key":"test"}]`)
	if result, payload = callTool(t, handleAgents, nil); result.IsError || len(agentRows(t, payload)) != 1 {
		t.Fatalf("optional webhook failure broke OMP discovery: %v", payload)
	}
	if result, payload = callTool(t, handleAgents, map[string]any{"include_grok": true}); !result.IsError {
		t.Fatalf("ambiguous webhook configuration was silently listed: %v", payload)
	}
}

func TestAgentJobViewKeepsObservedTitlesTimesAndActualStatus(t *testing.T) {
	newTestStack(t)
	start := time.Date(2026, 10, 17, 8, 0, 0, 0, time.UTC)
	end := start.Add(3 * time.Minute)
	globalStore.add(&Job{
		ID: "sub_observed", Status: "done", Steerable: true,
		Title: "Launch name", SessionTitle: "Observed session title",
		StartTime: start, EndTime: end, MetadataVersion: metadataVersion,
	})
	result, payload := callTool(t, handleAgent, map[string]any{"agent_id": "sub_observed"})
	if result.IsError {
		t.Fatal(payload)
	}
	row := payload["agent"].(map[string]any)
	if row["status"] != "done" || row["steerable"] != false || row["title"] != "Launch name" || row["session_title"] != "Observed session title" ||
		row["start_time"] != start.Format(time.RFC3339) || row["end_time"] != end.Format(time.RFC3339) {
		t.Fatalf("job discovery synthesized metadata or confused title with name: %v", row)
	}
	for _, absent := range []string{"last_seen", "last_seen_age_seconds"} {
		if _, exists := row[absent]; exists {
			t.Fatalf("execution timestamp fabricated a heartbeat: %v", row)
		}
	}
}

func TestAgentsHidesStaleUnlessRequested(t *testing.T) {
	srv := newTestStack(t)
	id, _ := registerTestAgent(t, srv, map[string]any{"machine": "mac", "ttl_seconds": 30})

	globalAgents.mu.Lock()
	globalAgents.agents[id].LastSeen = time.Now().Add(-2 * time.Minute)
	globalAgents.mu.Unlock()

	_, payload := callTool(t, handleAgents, map[string]any{})
	if rows := agentRows(t, payload); len(rows) != 0 {
		t.Fatalf("stale agent was listed: %v", rows)
	}
	_, withStale := callTool(t, handleAgents, map[string]any{"include_stale": true})
	rows := agentRows(t, withStale)
	if len(rows) != 1 || rows[0]["status"] != "stale" {
		t.Fatalf("include_stale returned %v", rows)
	}
}

func TestPromptQueuesForRegisteredAgentAndSurfacesResult(t *testing.T) {
	srv := newTestStack(t)
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "mac-mini", "session_id": "sess-mac"})

	_, payload := callTool(t, handlePrompt, map[string]any{
		"agent_id":     id,
		"text":         "run the failing test and report",
		"mode":         "follow_up",
		"wait_seconds": 0,
	})
	if payload["status"] != "queued" || payload["transport"] != "poll" || payload["mode"] != "follow_up" {
		t.Fatalf("unexpected prompt response: %v", payload)
	}
	deliveryID, _ := payload["delivery_id"].(string)
	if deliveryID == "" {
		t.Fatalf("prompt did not return a delivery id: %v", payload)
	}

	code, inbox := doJSON(t, http.MethodGet, srv.URL+"/api/agents/"+id+"/inbox?wait=0", token, nil)
	if code != http.StatusOK {
		t.Fatalf("inbox returned %d: %v", code, inbox)
	}
	items, _ := inbox["deliveries"].([]any)
	if len(items) != 1 {
		t.Fatalf("client received %d deliveries: %v", len(items), inbox)
	}
	first, _ := items[0].(map[string]any)
	if first["text"] != "run the failing test and report" || first["mode"] != "follow_up" {
		t.Fatalf("unexpected delivery: %v", first)
	}

	if code, ack := doJSON(t, http.MethodPost, srv.URL+"/api/agents/"+id+"/ack", token, map[string]any{
		"delivery_id": deliveryID,
		"status":      "ok",
		"result":      "test passes now",
	}); code != http.StatusOK {
		t.Fatalf("ack returned %d: %v", code, ack)
	}

	_, detail := callTool(t, handleAgent, map[string]any{"agent_id": id})
	if _, ok := detail["deliveries"]; ok {
		t.Fatalf("default agent detail must not include full deliveries: %v", detail)
	}
	if asInt(detail["acked"]) != 1 || asInt(detail["history_length"]) != 1 {
		t.Fatalf("expected acked=1 history_length=1, got %v", detail)
	}
	raw, _ := json.Marshal(detail["history_preview"])
	if !strings.Contains(string(raw), "test passes now") || !strings.Contains(string(raw), "acked") {
		t.Fatalf("agent summary does not preview the acked result: %s", raw)
	}

	_, full := callTool(t, handleAgent, map[string]any{"agent_id": id, "include_history": true})
	raw, _ = json.Marshal(full["deliveries"])
	if !strings.Contains(string(raw), "test passes now") || !strings.Contains(string(raw), "acked") {
		t.Fatalf("include_history does not carry the acked result: %s", raw)
	}
}

func TestPromptWaitsForPickup(t *testing.T) {
	srv := newTestStack(t)
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "mac-mini"})

	go func() {
		time.Sleep(200 * time.Millisecond)
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/agents/"+id+"/inbox?wait=5", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		res, err := http.DefaultClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}
	}()

	start := time.Now()
	_, payload := callTool(t, handlePrompt, map[string]any{
		"agent_id":     id,
		"text":         "status?",
		"wait_seconds": 5,
	})
	if payload["status"] != "delivered" || payload["delivered"] != true {
		t.Fatalf("prompt did not observe pickup: %v", payload)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("prompt waited %v, expected to return as soon as the client polled", elapsed)
	}
}

func TestPromptRejectsStaleAndUnknownAgents(t *testing.T) {
	srv := newTestStack(t)
	id, _ := registerTestAgent(t, srv, map[string]any{"machine": "mac", "ttl_seconds": 30})

	globalAgents.mu.Lock()
	globalAgents.agents[id].LastSeen = time.Now().Add(-2 * time.Minute)
	globalAgents.mu.Unlock()

	res, payload := callTool(t, handlePrompt, map[string]any{"agent_id": id, "text": "hello"})
	if !res.IsError || !strings.Contains(payload["__error_text"].(string), "stale") {
		t.Fatalf("prompt to a stale agent should fail: %v", payload)
	}
	res, payload = callTool(t, handlePrompt, map[string]any{"agent_id": "agt_nope", "text": "hello"})
	if !res.IsError {
		t.Fatalf("prompt to an unknown agent should fail: %v", payload)
	}
	res, _ = callTool(t, handlePrompt, map[string]any{"agent_id": id})
	if !res.IsError {
		t.Fatal("prompt without text should fail")
	}
	res, _ = callTool(t, handlePrompt, map[string]any{"agent_id": id, "text": "x", "mode": "shout"})
	if !res.IsError {
		t.Fatal("prompt with an unsupported mode should fail")
	}
}

func TestAgentResolvesBySessionAndRevokes(t *testing.T) {
	srv := newTestStack(t)
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "mac", "session_id": "sess-abc"})

	_, payload := callTool(t, handleAgent, map[string]any{"agent_id": "sess-abc"})
	agent, _ := payload["agent"].(map[string]any)
	if agent == nil || agent["agent_id"] != id {
		t.Fatalf("session lookup returned %v", payload)
	}

	_, revoked := callTool(t, handleAgent, map[string]any{"agent_id": id, "revoke": true})
	if revoked["revoked"] != true {
		t.Fatalf("revoke response: %v", revoked)
	}
	if code, _ := doJSON(t, http.MethodPost, srv.URL+"/api/agents/"+id+"/heartbeat", token, nil); code != http.StatusNotFound {
		t.Fatalf("revoked agent heartbeat returned %d, want 404", code)
	}
}

func TestSteerRequiresATarget(t *testing.T) {
	newTestStack(t)
	res, payload := callTool(t, handleSteer, map[string]any{"text": "hello"})
	if !res.IsError || !strings.Contains(payload["__error_text"].(string), "job_id is required") {
		t.Fatalf("steer without a target returned %v", payload)
	}
}

func TestPromptRefusesSenderOwnSession(t *testing.T) {
	srv := newTestStack(t)
	id, _ := registerTestAgent(t, srv, map[string]any{"machine": "mac", "session_id": "sess-self", "name": "mac:self"})
	for _, ref := range []string{id, "sess-self", "mac:self"} {
		res, payload := callTool(t, handlePrompt, map[string]any{"agent_id": ref, "text": "hi", "sender_session_id": "sess-self", "wait_seconds": 0})
		if !res.IsError || !strings.Contains(payload["__error_text"].(string), "your own session") {
			t.Fatalf("self prompt via %q was not refused: %v", ref, payload)
		}
	}
	if ag := globalAgents.get(id); ag == nil || len(ag.Queue) != 0 {
		t.Fatalf("refused self prompt was queued: %+v", ag)
	}
	res, payload := callTool(t, handlePrompt, map[string]any{"agent_id": id, "text": "hi", "sender_session_id": "sess-other", "wait_seconds": 0})
	if res.IsError {
		t.Fatalf("peer prompt was refused: %v", payload)
	}

	job := &Job{ID: "sub_self", Status: "running", Steerable: true, SessionID: "sess-job", PID: os.Getpid()}
	globalStore.add(job)
	for _, call := range []struct {
		name    string
		handler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error)
		args    map[string]any
	}{
		{"prompt", handlePrompt, map[string]any{"agent_id": "sess-job", "text": "hi", "sender_session_id": "sess-job"}},
		{"steer", handleSteer, map[string]any{"job_id": job.ID, "text": "hi", "sender_session_id": "sess-job"}},
	} {
		res, payload := callTool(t, call.handler, call.args)
		if !res.IsError || !strings.Contains(payload["__error_text"].(string), "your own session") {
			t.Fatalf("%s to own job session was not refused: %v", call.name, payload)
		}
	}
}

func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return -1
	}
}

func toolText(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	if len(res.Content) == 0 {
		t.Fatal("empty tool result")
	}
	tc, ok := res.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("content is %T", res.Content[0])
	}
	return tc.Text
}

func fillHugeDeliveries(t *testing.T, id string) {
	t.Helper()
	blob := strings.Repeat("Q", 32*1024)
	hist := strings.Repeat("H", 32*1024)
	globalAgents.mu.Lock()
	defer globalAgents.mu.Unlock()
	ag, ok := globalAgents.agents[id]
	if !ok {
		t.Fatalf("agent %s missing", id)
	}
	ag.Queue = ag.Queue[:0]
	ag.History = ag.History[:0]
	for i := range 50 {
		ag.Queue = append(ag.Queue, &Delivery{
			ID:     fmt.Sprintf("q%02d", i),
			Text:   blob,
			Mode:   "steer",
			Status: "queued",
		})
	}
	for i := range 20 {
		ag.History = append(ag.History, &Delivery{
			ID:     fmt.Sprintf("h%02d", i),
			Text:   hist,
			Result: hist,
			Mode:   "follow_up",
			Status: "acked",
		})
	}
}

func TestAgentDefaultResponseStaysSmall(t *testing.T) {
	srv := newTestStack(t)
	id, _ := registerTestAgent(t, srv, map[string]any{"machine": "mac", "session_id": "sess-huge"})
	fillHugeDeliveries(t, id)

	res, payload := callTool(t, handleAgent, map[string]any{"agent_id": id})
	raw := toolText(t, res)
	if len(raw) >= 6*1024 {
		t.Fatalf("default agent response is %d bytes, want < 6KB", len(raw))
	}
	if _, ok := payload["queue"]; ok {
		t.Fatalf("default response must not include full queue")
	}
	if _, ok := payload["deliveries"]; ok {
		t.Fatalf("default response must not include full deliveries")
	}
	if asInt(payload["queued"]) != 50 || asInt(payload["acked"]) != 20 || asInt(payload["history_length"]) != 20 {
		t.Fatalf("counts: queued=%v acked=%v history_length=%v", payload["queued"], payload["acked"], payload["history_length"])
	}
	qPrev, _ := payload["queue_preview"].([]any)
	if len(qPrev) == 0 || len(qPrev) > agentPreviewLimit {
		t.Fatalf("queue_preview length %d", len(qPrev))
	}
	firstQ, _ := qPrev[0].(map[string]any)
	if text, _ := firstQ["text"].(string); len(text) > previewMaxChars {
		t.Fatalf("queue preview text is %d chars, want <= %d", len(text), previewMaxChars)
	}
	if firstQ["truncated"] != true {
		t.Fatalf("huge queue preview should be marked truncated: %v", firstQ)
	}

	_, queued := callTool(t, handleAgent, map[string]any{"agent_id": id, "include_queue": true})
	items, _ := queued["queue"].([]any)
	if len(items) != 50 {
		t.Fatalf("include_queue returned %d items, want 50", len(items))
	}
	for i, item := range items {
		row, _ := item.(map[string]any)
		text, _ := row["text"].(string)
		if len(text) > defaultMaxChars {
			t.Fatalf("item %d text is %d chars, want <= %d", i, len(text), defaultMaxChars)
		}
		if row["truncated"] != true {
			t.Fatalf("item %d should be marked truncated", i)
		}
		if asInt(row["text_chars"]) != 32*1024 {
			t.Fatalf("item %d text_chars=%v, want %d", i, row["text_chars"], 32*1024)
		}
	}

	_, custom := callTool(t, handleAgent, map[string]any{"agent_id": id, "include_queue": true, "max_chars": 100})
	items, _ = custom["queue"].([]any)
	if len(items) != 50 {
		t.Fatalf("custom max_chars queue length %d", len(items))
	}
	row, _ := items[0].(map[string]any)
	if text, _ := row["text"].(string); len(text) != 100 {
		t.Fatalf("max_chars=100 produced %d char text", len(text))
	}

	_, ceiling := callTool(t, handleAgent, map[string]any{"agent_id": id, "include_queue": true, "max_chars": 99999})
	items, _ = ceiling["queue"].([]any)
	row, _ = items[0].(map[string]any)
	if text, _ := row["text"].(string); len(text) != hardMaxChars {
		t.Fatalf("max_chars ceiling produced %d char text, want %d", len(text), hardMaxChars)
	}
}

func TestPromptAckResultIsCapped(t *testing.T) {
	srv := newTestStack(t)
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "mac"})
	huge := strings.Repeat("R", 8000)

	go func() {
		time.Sleep(150 * time.Millisecond)
		code, inbox := doJSON(t, http.MethodGet, srv.URL+"/api/agents/"+id+"/inbox?wait=5", token, nil)
		if code != http.StatusOK {
			return
		}
		items, _ := inbox["deliveries"].([]any)
		if len(items) == 0 {
			return
		}
		first, _ := items[0].(map[string]any)
		_, _ = doJSON(t, http.MethodPost, srv.URL+"/api/agents/"+id+"/ack", token, map[string]any{
			"delivery_id": first["id"],
			"status":      "ok",
			"result":      huge,
		})
	}()

	_, payload := callTool(t, handlePrompt, map[string]any{
		"agent_id":     id,
		"text":         "summarize",
		"wait_for":     "ack",
		"wait_seconds": 5,
	})
	result, _ := payload["result"].(string)
	if len(result) > defaultMaxChars {
		t.Fatalf("ack result is %d chars, want <= %d", len(result), defaultMaxChars)
	}
	if payload["truncated"] != true {
		t.Fatalf("expected truncated ack result, got %v", payload)
	}
	if !strings.HasPrefix(result, "RRR") {
		t.Fatalf("ack result should be head-cut, got %q", result[:min(32, len(result))])
	}
}
