package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testEnrollToken = "enroll-token-aaaa"

func newTestRegistry(t *testing.T, tokens string) (*AgentRegistry, *httptest.Server) {
	t.Helper()
	t.Setenv("SUPERVISOR_REGISTRY_TOKEN", tokens)
	registry, err := newAgentRegistry(t.TempDir())
	if err != nil {
		t.Fatalf("newAgentRegistry: %v", err)
	}
	globalAgents = registry
	srv := httptest.NewServer(registry.registryMux())
	t.Cleanup(srv.Close)
	return registry, srv
}

func doJSON(t *testing.T, method, url, token string, body any) (int, map[string]any) {
	t.Helper()
	var reader *strings.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = strings.NewReader(string(raw))
	} else {
		reader = strings.NewReader("")
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer res.Body.Close()
	payload := map[string]any{}
	_ = json.NewDecoder(res.Body).Decode(&payload)
	return res.StatusCode, payload
}

func registerTestAgent(t *testing.T, srv *httptest.Server, body map[string]any) (string, string) {
	t.Helper()
	code, payload := doJSON(t, http.MethodPost, srv.URL+"/api/agents/register", testEnrollToken, body)
	if code != http.StatusOK {
		t.Fatalf("register returned %d: %v", code, payload)
	}
	id, _ := payload["agent_id"].(string)
	token, _ := payload["agent_token"].(string)
	if id == "" || token == "" {
		t.Fatalf("register did not return an id and token: %v", payload)
	}
	return id, token
}

func TestRegisterRequiresEnrollmentToken(t *testing.T) {
	_, srv := newTestRegistry(t, testEnrollToken)

	if code, _ := doJSON(t, http.MethodPost, srv.URL+"/api/agents/register", "", map[string]any{"machine": "mac"}); code != http.StatusUnauthorized {
		t.Fatalf("anonymous register returned %d, want 401", code)
	}
	if code, _ := doJSON(t, http.MethodPost, srv.URL+"/api/agents/register", "wrong-token", map[string]any{"machine": "mac"}); code != http.StatusUnauthorized {
		t.Fatalf("bad-token register returned %d, want 401", code)
	}
	registerTestAgent(t, srv, map[string]any{"machine": "mac", "name": "mac:project"})
}

func TestRegisterAcceptsRotatedToken(t *testing.T) {
	_, srv := newTestRegistry(t, testEnrollToken+",next-token-bbbb")

	for _, token := range []string{testEnrollToken, "next-token-bbbb"} {
		code, payload := doJSON(t, http.MethodPost, srv.URL+"/api/agents/register", token, map[string]any{"machine": "mac"})
		if code != http.StatusOK {
			t.Fatalf("token %q returned %d: %v", token, code, payload)
		}
	}
	if code, _ := doJSON(t, http.MethodPost, srv.URL+"/api/agents/register", "retired-token", map[string]any{"machine": "mac"}); code != http.StatusUnauthorized {
		t.Fatalf("retired token returned %d, want 401", code)
	}
}

func TestRegisterHonorsMachineAllowlist(t *testing.T) {
	t.Setenv("SUPERVISOR_REGISTRY_ALLOW_MACHINES", "mac-*,hub-host")
	_, srv := newTestRegistry(t, testEnrollToken)

	if code, _ := doJSON(t, http.MethodPost, srv.URL+"/api/agents/register", testEnrollToken, map[string]any{"machine": "mac-mini"}); code != http.StatusOK {
		t.Fatalf("allowlisted machine returned %d, want 200", code)
	}
	code, payload := doJSON(t, http.MethodPost, srv.URL+"/api/agents/register", testEnrollToken, map[string]any{"machine": "attacker-box"})
	if code != http.StatusForbidden {
		t.Fatalf("non-allowlisted machine returned %d, want 403 (%v)", code, payload)
	}
}
func TestRegisterConflictsOnCrossHarnessLiveTakeover(t *testing.T) {
	_, srv := newTestRegistry(t, testEnrollToken)
	withHarness := func(harness string) map[string]any {
		return map[string]any{"machine": "mac", "name": "work", "session_id": "sess-1", "harness": harness}
	}
	id, _ := registerTestAgent(t, srv, withHarness("omp-tty"))

	code, payload := doJSON(t, http.MethodPost, srv.URL+"/api/agents/register", testEnrollToken, withHarness("omp-tty"))
	if code != http.StatusOK {
		t.Fatalf("same-harness re-register returned %d, want 200 (%v)", code, payload)
	}
	if payload["agent_id"] != id {
		t.Fatalf("same-harness re-register changed identity to %v, want %s", payload["agent_id"], id)
	}

	code, payload = doJSON(t, http.MethodPost, srv.URL+"/api/agents/register", testEnrollToken, withHarness("omp-rpc-bridge"))
	if code != http.StatusConflict {
		t.Fatalf("cross-harness takeover returned %d, want 409 (%v)", code, payload)
	}
	if payload["code"] != "conflict" {
		t.Fatalf("cross-harness takeover code = %v, want conflict", payload["code"])
	}

	code, payload = doJSON(t, http.MethodPost, srv.URL+"/api/agents/register", testEnrollToken, map[string]any{
		"agent_id": id, "machine": "mac", "name": "work", "session_id": "sess-1", "harness": "omp-rpc-bridge",
	})
	if code != http.StatusOK {
		t.Fatalf("explicit resume returned %d, want 200 (%v)", code, payload)
	}
	if payload["agent_id"] != id {
		t.Fatalf("explicit resume changed identity to %v, want %s", payload["agent_id"], id)
	}
}

func TestRegistryGeneratesTokenWhenUnset(t *testing.T) {
	t.Setenv("SUPERVISOR_REGISTRY_TOKEN", "")
	dir := t.TempDir()
	registry, err := newAgentRegistry(dir)
	if err != nil {
		t.Fatalf("newAgentRegistry: %v", err)
	}
	tokenPath := filepath.Join(dir, "registry-token")
	info, err := os.Stat(tokenPath)
	if err != nil {
		t.Fatalf("expected a generated token at %s: %v", tokenPath, err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Fatalf("token file mode is %v, want 0600", perm)
	}
	data, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(string(data))
	if !registry.auth.acceptsEnrollToken(token) {
		t.Fatal("generated token is not accepted by the registry")
	}
	if registry.auth.acceptsEnrollToken("") || registry.auth.acceptsEnrollToken("nope") {
		t.Fatal("registry accepted an invalid token")
	}
}

func TestRegistryPersistsOnlyTokenHashes(t *testing.T) {
	registry, srv := newTestRegistry(t, testEnrollToken)
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "mac", "session_id": "sess-1"})

	data, err := os.ReadFile(filepath.Join(registry.dir, id+".json"))
	if err != nil {
		t.Fatalf("agent file: %v", err)
	}
	if strings.Contains(string(data), token) {
		t.Fatal("agent record contains the plaintext agent token")
	}
	if !strings.Contains(string(data), hashToken(token)) {
		t.Fatal("agent record does not contain the token hash")
	}
}

func TestHeartbeatRefreshesAndForeignTokenRejected(t *testing.T) {
	registry, srv := newTestRegistry(t, testEnrollToken)
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "mac", "ttl_seconds": 30})
	otherID, otherToken := registerTestAgent(t, srv, map[string]any{"machine": "hub-host"})

	registry.mu.Lock()
	registry.agents[id].LastSeen = time.Now().Add(-20 * time.Second)
	registry.mu.Unlock()

	code, payload := doJSON(t, http.MethodPost, srv.URL+"/api/agents/"+id+"/heartbeat", token, map[string]any{"model": "cursor/cursor-grok-4.6", "title": "live work", "busy": true})
	if code != http.StatusOK {
		t.Fatalf("heartbeat returned %d: %v", code, payload)
	}
	ag := registry.get(id)
	if ag.status(time.Now()) != "live" {
		t.Fatalf("agent is %s after heartbeat, want live", ag.status(time.Now()))
	}
	if ag.Model != "cursor/cursor-grok-4.6" || ag.Title != "live work" || !ag.Busy {
		t.Fatalf("heartbeat metadata not applied: %+v", ag)
	}

	if code, _ := doJSON(t, http.MethodPost, srv.URL+"/api/agents/"+id+"/heartbeat", otherToken, nil); code != http.StatusUnauthorized {
		t.Fatalf("foreign agent token returned %d, want 401", code)
	}
	if code, _ := doJSON(t, http.MethodGet, srv.URL+"/api/agents/"+otherID+"/inbox?wait=0", token, nil); code != http.StatusUnauthorized {
		t.Fatalf("cross-agent inbox read returned %d, want 401", code)
	}
	if code, payload := doJSON(t, http.MethodPost, srv.URL+"/api/agents/agt_missing/heartbeat", token, nil); code != http.StatusNotFound || payload["code"] != "unknown_agent" {
		t.Fatalf("unknown agent returned %d/%v, want 404/unknown_agent", code, payload)
	}
}

func TestHeartbeatPreservesOmittedObservationsAndClearsExplicitBlanks(t *testing.T) {
	registry, srv := newTestRegistry(t, testEnrollToken)
	id, token := registerTestAgent(t, srv, map[string]any{
		"machine": "mac", "session_id": "previous-session", "model": "previous-model",
		"title": "Previous title", "cwd": "/workspace",
	})
	url := srv.URL + "/api/agents/" + id + "/heartbeat"
	if code, payload := doJSON(t, http.MethodPost, url, token, map[string]any{"busy": false}); code != http.StatusOK {
		t.Fatalf("metadata-free heartbeat returned %d: %v", code, payload)
	}
	if got := registry.get(id); got.Title != "Previous title" || got.Model != "previous-model" || got.SessionID != "previous-session" {
		t.Fatalf("omitted observations changed: title=%q model=%q session_id=%q", got.Title, got.Model, got.SessionID)
	}
	if code, payload := doJSON(t, http.MethodPost, url, token, map[string]any{
		"title": "", "model": "", "session_id": "",
	}); code != http.StatusOK {
		t.Fatalf("clearing heartbeat returned %d: %v", code, payload)
	}
	if got := registry.get(id); got.Title != "" || got.Model != "" || got.SessionID != "" || got.CWD != "/workspace" {
		t.Fatalf("explicit clearing failed: title=%q model=%q session_id=%q cwd=%q", got.Title, got.Model, got.SessionID, got.CWD)
	}
}

func TestAgentHostIdentityDoesNotReplaceExecutionAliasOrCapabilities(t *testing.T) {
	registry, srv := newTestRegistry(t, testEnrollToken)
	id, token := registerTestAgent(t, srv, map[string]any{
		"machine":      "mac-launch-alias",
		"session_id":   "host-identity-session",
		"capabilities": []string{"prompt", "result"},
		"host":         map[string]any{"hostname": "  devbook.local  ", "platform": "macos", "architecture": "aarch64"},
	})
	registered := registry.get(id)
	want := hostIdentity{Hostname: "devbook.local", Platform: "darwin", Architecture: "arm64"}
	if registered.Host == nil || *registered.Host != want || registered.Machine != "mac-launch-alias" {
		t.Fatalf("host identity changed alias or was not normalized: %+v", registered)
	}
	if strings.Join(registered.Capabilities, ",") != "prompt,result" {
		t.Fatalf("host enrollment changed capabilities: %v", registered.Capabilities)
	}

	registered.Host.Hostname = "mutated-snapshot"
	if got := registry.get(id); got.Host == nil || *got.Host != want {
		t.Fatalf("snapshot shares mutable host identity: %+v", got.Host)
	}
	heartbeatURL := srv.URL + "/api/agents/" + id + "/heartbeat"
	if code, payload := doJSON(t, http.MethodPost, heartbeatURL, token, map[string]any{
		"machine": "not-the-alias", "capabilities": []string{"invented"},
		"host": map[string]any{"hostname": "worker-02", "platform": "windows", "architecture": "amd64"},
	}); code != http.StatusOK {
		t.Fatalf("host heartbeat returned %d: %v", code, payload)
	}
	want = hostIdentity{Hostname: "worker-02", Platform: "win32", Architecture: "x64"}
	updated := registry.get(id)
	if updated.Machine != "mac-launch-alias" || updated.Host == nil || *updated.Host != want ||
		strings.Join(updated.Capabilities, ",") != "prompt,result" {
		t.Fatalf("heartbeat confused host identity, alias or capabilities: %+v", updated)
	}
	if registered.Host.Hostname != "mutated-snapshot" || registered.Host.Platform != "darwin" {
		t.Fatalf("heartbeat mutated an earlier snapshot: %+v", registered.Host)
	}
	if code, payload := doJSON(t, http.MethodPost, heartbeatURL, token, map[string]any{"title": "legacy heartbeat"}); code != http.StatusOK {
		t.Fatalf("legacy heartbeat returned %d: %v", code, payload)
	}
	if got := registry.get(id); got.Host == nil || *got.Host != want {
		t.Fatalf("omitted host metadata was erased: %+v", got.Host)
	}

	registry.mu.Lock()
	registry.agents[id].lastSave = time.Time{}
	registry.mu.Unlock()
	if code, payload := doJSON(t, http.MethodPost, heartbeatURL, token, nil); code != http.StatusOK {
		t.Fatalf("persisting heartbeat returned %d: %v", code, payload)
	}
	reloaded, err := newAgentRegistry(filepath.Dir(registry.dir))
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.get(id); got == nil || got.Host == nil || *got.Host != want || got.Machine != "mac-launch-alias" {
		t.Fatalf("persisted host identity lost: %+v", got)
	}
}

func TestAgentHostMetadataIsOptionalBoundedAndExplicitlyReplaceable(t *testing.T) {
	registry, srv := newTestRegistry(t, testEnrollToken)
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "legacy-alias"})
	if got := registry.get(id); got.Host != nil {
		t.Fatalf("legacy registration acquired invented host metadata: %+v", got.Host)
	}
	heartbeatURL := srv.URL + "/api/agents/" + id + "/heartbeat"
	if code, payload := doJSON(t, http.MethodPost, heartbeatURL, token, map[string]any{
		"host": map[string]any{"hostname": strings.Repeat("é", 300), "platform": "unknown", "architecture": "riscv64"},
	}); code != http.StatusOK {
		t.Fatalf("bounded host heartbeat returned %d: %v", code, payload)
	}
	got := registry.get(id)
	if got.Host == nil || got.Host.Hostname != strings.Repeat("é", 255) || got.Host.Platform != "" || got.Host.Architecture != "" {
		t.Fatalf("untrusted metadata bounds or enums were bypassed: %+v", got.Host)
	}
	if code, payload := doJSON(t, http.MethodPost, heartbeatURL, token, map[string]any{"host": map[string]any{}}); code != http.StatusOK {
		t.Fatalf("clearing host returned %d: %v", code, payload)
	}
	if got := registry.get(id); got.Host != nil || got.Machine != "legacy-alias" {
		t.Fatalf("empty identity did not clear host independently of alias: %+v", got)
	}
	if code, _ := doJSON(t, http.MethodPost, heartbeatURL, token, map[string]any{"host": "not-an-object"}); code != http.StatusBadRequest {
		t.Fatalf("invalid host shape returned %d, want 400", code)
	}
}

func TestStaleRegistrationExpiresAndIsReaped(t *testing.T) {
	registry, srv := newTestRegistry(t, testEnrollToken)
	id, _ := registerTestAgent(t, srv, map[string]any{"machine": "mac", "ttl_seconds": 30})

	registry.mu.Lock()
	registry.agents[id].LastSeen = time.Now().Add(-45 * time.Second)
	registry.mu.Unlock()

	if got := registry.get(id).status(time.Now()); got != "stale" {
		t.Fatalf("status is %q, want stale", got)
	}
	if n := registry.reap(); n != 0 {
		t.Fatalf("reaped %d agents while inside the grace window", n)
	}

	registry.mu.Lock()
	registry.agents[id].LastSeen = time.Now().Add(-(30*time.Second + agentGrace + time.Second))
	registry.mu.Unlock()

	if n := registry.reap(); n != 1 {
		t.Fatalf("reaped %d agents, want 1", n)
	}
	if registry.get(id) != nil {
		t.Fatal("expired registration is still listed")
	}
	if _, err := os.Stat(filepath.Join(registry.dir, id+".json")); !os.IsNotExist(err) {
		t.Fatal("expired registration file was not removed")
	}
}

func TestInboxLongPollDeliversQueuedPromptOnce(t *testing.T) {
	registry, srv := newTestRegistry(t, testEnrollToken)
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "mac"})

	type pollResult struct {
		code    int
		payload map[string]any
	}
	results := make(chan pollResult, 1)
	go func() {
		code, payload := doJSON(t, http.MethodGet, srv.URL+"/api/agents/"+id+"/inbox?wait=10", token, nil)
		results <- pollResult{code, payload}
	}()

	time.Sleep(150 * time.Millisecond)
	delivery, err := registry.enqueue(id, "check the failing test", "steer", "Main", nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	select {
	case res := <-results:
		if res.code != http.StatusOK {
			t.Fatalf("inbox returned %d: %v", res.code, res.payload)
		}
		items, _ := res.payload["deliveries"].([]any)
		if len(items) != 1 {
			t.Fatalf("inbox returned %d deliveries, want 1: %v", len(items), res.payload)
		}
		first, _ := items[0].(map[string]any)
		if first["id"] != delivery.ID || first["text"] != "check the failing test" || first["mode"] != "steer" {
			t.Fatalf("unexpected delivery payload: %v", first)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long-poll did not return after a prompt was queued")
	}

	if got := registry.findDelivery(id, delivery.ID); got == nil || got.Status != "delivered" {
		t.Fatalf("delivery status is %+v, want delivered", got)
	}

	code, payload := doJSON(t, http.MethodGet, srv.URL+"/api/agents/"+id+"/inbox?wait=0", token, nil)
	if code != http.StatusOK {
		t.Fatalf("second poll returned %d: %v", code, payload)
	}
	if items, _ := payload["deliveries"].([]any); len(items) != 0 {
		t.Fatalf("second poll replayed %d deliveries", len(items))
	}
}

func TestAckRecordsResultAndUnregisterRemovesAgent(t *testing.T) {
	registry, srv := newTestRegistry(t, testEnrollToken)
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "mac"})

	delivery, err := registry.enqueue(id, "summarize the diff", "steer", "Main", nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if code, payload := doJSON(t, http.MethodGet, srv.URL+"/api/agents/"+id+"/inbox?wait=0", token, nil); code != http.StatusOK {
		t.Fatalf("inbox returned %d: %v", code, payload)
	}

	code, payload := doJSON(t, http.MethodPost, srv.URL+"/api/agents/"+id+"/ack", token, map[string]any{"delivery_id": delivery.ID, "status": "ok", "result": "three files changed"})
	if code != http.StatusOK {
		t.Fatalf("ack returned %d: %v", code, payload)
	}
	got := registry.findDelivery(id, delivery.ID)
	if got == nil || got.Status != "acked" || got.Result != "three files changed" {
		t.Fatalf("ack not recorded: %+v", got)
	}

	if code, payload := doJSON(t, http.MethodPost, srv.URL+"/api/agents/"+id+"/ack", token, map[string]any{"delivery_id": "dlv_nope", "status": "ok"}); code != http.StatusNotFound {
		t.Fatalf("unknown delivery ack returned %d: %v", code, payload)
	}

	if code, payload := doJSON(t, http.MethodPost, srv.URL+"/api/agents/"+id+"/unregister", token, nil); code != http.StatusOK {
		t.Fatalf("unregister returned %d: %v", code, payload)
	}
	if registry.get(id) != nil {
		t.Fatal("agent is still registered after unregister")
	}

	if code, _ := doJSON(t, http.MethodPost, srv.URL+"/api/agents/"+id+"/heartbeat", token, nil); code != http.StatusNotFound {
		t.Fatalf("heartbeat after unregister returned %d, want 404", code)
	}
}

func TestReRegistrationReplacesSameSession(t *testing.T) {
	registry, srv := newTestRegistry(t, testEnrollToken)
	id, firstToken := registerTestAgent(t, srv, map[string]any{"machine": "mac", "session_id": "sess-7"})
	againID, secondToken := registerTestAgent(t, srv, map[string]any{"machine": "mac", "session_id": "sess-7"})

	if againID != id {
		t.Fatalf("re-registration created %s instead of reusing %s", againID, id)
	}
	if len(registry.list()) != 1 {
		t.Fatalf("registry holds %d agents, want 1", len(registry.list()))
	}
	if code, _ := doJSON(t, http.MethodPost, srv.URL+"/api/agents/"+id+"/heartbeat", firstToken, nil); code != http.StatusUnauthorized {
		t.Fatalf("superseded token returned %d, want 401", code)
	}
	if code, _ := doJSON(t, http.MethodPost, srv.URL+"/api/agents/"+id+"/heartbeat", secondToken, nil); code != http.StatusOK {
		t.Fatalf("current token returned %d, want 200", code)
	}
}

func TestRegistrationSurvivesRestart(t *testing.T) {
	t.Setenv("SUPERVISOR_REGISTRY_TOKEN", testEnrollToken)
	dir := t.TempDir()
	registry, err := newAgentRegistry(dir)
	if err != nil {
		t.Fatalf("newAgentRegistry: %v", err)
	}
	globalAgents = registry
	srv := httptest.NewServer(registry.registryMux())
	defer srv.Close()
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "mac", "session_id": "sess-restart"})

	reloaded, err := newAgentRegistry(dir)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	globalAgents = reloaded
	restarted := httptest.NewServer(reloaded.registryMux())
	defer restarted.Close()

	if reloaded.get(id) == nil {
		t.Fatal("registration did not survive the restart")
	}
	if code, payload := doJSON(t, http.MethodPost, restarted.URL+"/api/agents/"+id+"/heartbeat", token, nil); code != http.StatusOK {
		t.Fatalf("heartbeat after restart returned %d: %v", code, payload)
	}
	if _, err := reloaded.enqueue(id, "still there?", "steer", "", nil); err != nil {
		t.Fatalf("enqueue after restart: %v", err)
	}
}

func TestRegistryDisabledRejectsEverything(t *testing.T) {
	t.Setenv("SUPERVISOR_REGISTRY_DISABLED", "1")
	_, srv := newTestRegistry(t, testEnrollToken)

	code, payload := doJSON(t, http.MethodPost, srv.URL+"/api/agents/register", testEnrollToken, map[string]any{"machine": "mac"})
	if code != http.StatusServiceUnavailable || payload["code"] != "registry_disabled" {
		t.Fatalf("disabled registry returned %d/%v, want 503/registry_disabled", code, payload)
	}
}

func TestEnqueueLimits(t *testing.T) {
	registry, srv := newTestRegistry(t, testEnrollToken)
	id, _ := registerTestAgent(t, srv, map[string]any{"machine": "mac"})

	if _, err := registry.enqueue(id, strings.Repeat("x", maxDeliveryTextBytes+1), "steer", "", nil); err == nil {
		t.Fatal("oversized prompt was accepted")
	}
	for i := range maxQueuedDeliveries {
		if _, err := registry.enqueue(id, fmt.Sprintf("prompt %d", i), "steer", "", nil); err != nil {
			t.Fatalf("enqueue %d failed: %v", i, err)
		}
	}
	if _, err := registry.enqueue(id, "one too many", "steer", "", nil); err == nil {
		t.Fatal("queue limit was not enforced")
	}
	if _, err := registry.enqueue("agt_missing", "hello", "steer", "", nil); err != errUnknownAgent {
		t.Fatalf("enqueue to unknown agent returned %v, want errUnknownAgent", err)
	}
}
