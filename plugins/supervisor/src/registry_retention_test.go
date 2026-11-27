package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func deliverAndAck(t *testing.T, registry *AgentRegistry, url, id, token, text string) string {
	t.Helper()
	d, err := registry.enqueue(id, text, "steer", "peer", nil)
	if err != nil {
		t.Fatalf("enqueue %q: %v", text, err)
	}
	if code, payload := doJSON(t, http.MethodGet, url+"/api/agents/"+id+"/inbox?wait=0", token, nil); code != http.StatusOK {
		t.Fatalf("inbox returned %d: %v", code, payload)
	}
	if code, payload := doJSON(t, http.MethodPost, url+"/api/agents/"+id+"/ack", token, map[string]any{"delivery_id": d.ID, "status": "ok"}); code != http.StatusOK {
		t.Fatalf("ack %s returned %d: %v", d.ID, code, payload)
	}
	return d.ID
}

func settledHistory(ag *Agent) int {
	n := 0
	for _, d := range ag.History {
		if d.Status != "delivered" {
			n++
		}
	}
	return n
}

func TestUnackedDeliverySurvivesNewerDeliveries(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SUPERVISOR_REGISTRY_TOKEN", testEnrollToken)
	registry, err := newAgentRegistry(dir)
	if err != nil {
		t.Fatalf("newAgentRegistry: %v", err)
	}
	globalAgents = registry
	srv := httptest.NewServer(registry.registryMux())
	defer srv.Close()
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "mac", "session_id": "sess-long"})

	long, err := registry.enqueue(id, "refactor the whole module", "follow_up", "", nil)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if code, payload := doJSON(t, http.MethodGet, srv.URL+"/api/agents/"+id+"/inbox?wait=0", token, nil); code != http.StatusOK {
		t.Fatalf("inbox returned %d: %v", code, payload)
	}
	for i := range maxDeliveryHistory + 15 {
		deliverAndAck(t, registry, srv.URL, id, token, fmt.Sprintf("peer note %d", i))
	}

	if got := registry.findDelivery(id, long.ID); got == nil || got.Status != "delivered" {
		t.Fatalf("unacked delivery was evicted or changed: %+v", got)
	}
	if ag := registry.get(id); settledHistory(ag) != maxDeliveryHistory {
		t.Fatalf("settled history = %d, want %d", settledHistory(ag), maxDeliveryHistory)
	}

	reloaded, err := newAgentRegistry(dir)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	globalAgents = reloaded
	restarted := httptest.NewServer(reloaded.registryMux())
	defer restarted.Close()
	if got := reloaded.findDelivery(id, long.ID); got == nil || got.Status != "delivered" {
		t.Fatalf("unacked delivery lost across restart: %+v", got)
	}

	code, payload := doJSON(t, http.MethodPost, restarted.URL+"/api/agents/"+id+"/ack", token, map[string]any{"delivery_id": long.ID, "status": "ok", "result": "module refactored"})
	if code != http.StatusOK {
		t.Fatalf("late ack returned %d: %v", code, payload)
	}
	final := waitForDelivery(context.Background(), id, long.ID, "ack", time.Second)
	if final.Status != "acked" || final.Result != "module refactored" {
		t.Fatalf("late reply not matched: %+v", final)
	}
	if ag := reloaded.get(id); settledHistory(ag) != maxDeliveryHistory || len(ag.History) != maxDeliveryHistory {
		t.Fatalf("history after late ack: settled=%d len=%d, want %d", settledHistory(ag), len(ag.History), maxDeliveryHistory)
	}
}

func TestDeliveryHistoryStaysBounded(t *testing.T) {
	registry, srv := newTestRegistry(t, testEnrollToken)
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "mac"})

	var ids []string
	for i := range maxDeliveryHistory + 10 {
		ids = append(ids, deliverAndAck(t, registry, srv.URL, id, token, fmt.Sprintf("note %d", i)))
	}
	ag := registry.get(id)
	if len(ag.History) != maxDeliveryHistory {
		t.Fatalf("history length = %d, want %d", len(ag.History), maxDeliveryHistory)
	}
	if registry.findDelivery(id, ids[0]) != nil || registry.findDelivery(id, ids[9]) != nil {
		t.Fatal("oldest acked deliveries were not evicted")
	}
	if registry.findDelivery(id, ids[10]) == nil || registry.findDelivery(id, ids[len(ids)-1]) == nil {
		t.Fatal("newest acked deliveries were evicted")
	}

	var unacked []string
	for len(unacked) < maxUnackedDeliveries+10 {
		for range min(maxQueuedDeliveries, maxUnackedDeliveries+10-len(unacked)) {
			d, err := registry.enqueue(id, "never acked", "steer", "peer", nil)
			if err != nil {
				t.Fatalf("enqueue: %v", err)
			}
			unacked = append(unacked, d.ID)
		}
		if code, payload := doJSON(t, http.MethodGet, srv.URL+"/api/agents/"+id+"/inbox?wait=0", token, nil); code != http.StatusOK {
			t.Fatalf("inbox returned %d: %v", code, payload)
		}
	}
	ag = registry.get(id)
	if want := maxDeliveryHistory + maxUnackedDeliveries; len(ag.History) != want {
		t.Fatalf("history length = %d, want %d", len(ag.History), want)
	}
	if registry.findDelivery(id, unacked[0]) != nil || registry.findDelivery(id, unacked[10]) == nil {
		t.Fatal("unacked overflow did not drop exactly the oldest entries")
	}
	if settledHistory(ag) != maxDeliveryHistory {
		t.Fatalf("unacked overflow evicted settled entries: settled=%d", settledHistory(ag))
	}
}

func TestTrimDeliveryHistoryOnRestore(t *testing.T) {
	t.Setenv("SUPERVISOR_REGISTRY_TOKEN", testEnrollToken)
	dir := t.TempDir()
	registry, err := newAgentRegistry(dir)
	if err != nil {
		t.Fatalf("newAgentRegistry: %v", err)
	}
	ag := &Agent{ID: "agt_restore", TokenSHA256: "x", LastSeen: time.Now()}
	for i := range maxDeliveryHistory + 5 {
		ag.History = append(ag.History, &Delivery{ID: fmt.Sprintf("a%02d", i), Status: "acked"})
	}
	ag.History = append(ag.History, &Delivery{ID: "pending", Status: "delivered"})
	registry.mu.Lock()
	registry.saveLocked(ag)
	registry.mu.Unlock()

	reloaded, err := newAgentRegistry(filepath.Dir(registry.dir))
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got := reloaded.get("agt_restore")
	if got == nil || len(got.History) != maxDeliveryHistory+1 || got.History[0].ID != "a05" || got.History[len(got.History)-1].ID != "pending" {
		t.Fatalf("restored history not trimmed consistently: %+v", got)
	}
}
