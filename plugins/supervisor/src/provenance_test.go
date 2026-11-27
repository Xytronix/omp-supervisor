package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRPCPromptSourcePreservesHumanAndPeerInputs(t *testing.T) {
	tempDir := newTestStores(t)
	childStdin, supervisorWrites := io.Pipe()
	supervisorReads, childStdout := io.Pipe()
	fake := newFakeRPCChild(childStdin, childStdout)
	logFile, err := os.Create(filepath.Join(tempDir, "provenance-rpc.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	job := &Job{ID: "sub_provenance", PID: os.Getpid(), Status: "running", Steerable: true, StartTime: time.Now(), CWD: tempDir}

	if err := json.Unmarshal([]byte(`{"source":{"kind":"agent","from":"LaunchPeer"}}`), job); err != nil {
		t.Fatal(err)
	}
	child := startRPCChild(globalStore, job, supervisorWrites, supervisorReads, logFile, "initial assignment")
	job.rpc = child
	globalStore.add(job)
	defer func() {
		child.close()
		childStdout.Close()
		childStdin.Close()
		<-child.readDone
	}()
	fake.setState(rpcState{IsStreaming: true})
	fake.emit(map[string]any{"type": "ready", "protocolVersion": 1})
	initial := fake.waitForCommand(t, "prompt")
	if !reflect.DeepEqual(initial["source"], map[string]any{"kind": "agent", "from": "LaunchPeer"}) {
		t.Errorf("initial assignment lost peer provenance: %v", initial)
	}

	peer := map[string]any{"kind": "agent", "from": "Peer", "name": "Researcher", "title": "Observed session", "route": "omp"}
	for index, source := range []any{nil, peer, nil} {
		args := map[string]any{"agent_id": job.ID, "text": "same text", "from": "Peer", "mode": "follow_up"}
		if source != nil {
			args["source"] = source
		}
		result, payload := callTool(t, handlePrompt, args)
		if result.IsError {
			t.Fatalf("prompt failed: %v", payload)
		}
		frame := fake.waitForNthCommand(t, "prompt", index+2)
		if frame["message"] != "same text" || frame["streamingBehavior"] != "followUp" || !reflect.DeepEqual(frame["source"], source) {
			t.Errorf("prompt %d changed content, order, mode, or provenance: %v", index, frame)
		}
	}
	result, payload := callTool(t, handleSteer, map[string]any{"job_id": job.ID, "text": "urgent", "source": peer})
	if result.IsError {
		t.Fatalf("steer failed: %v", payload)
	}
	frame := fake.waitForNthCommand(t, "prompt", 5)
	if frame["message"] != "urgent" || frame["streamingBehavior"] != "steer" || !reflect.DeepEqual(frame["source"], peer) {
		t.Errorf("direct steering lost peer provenance: %v", frame)
	}
	for index, mode := range []string{"steer", "aside", "follow_up"} {
		result, payload := callTool(t, handlePrompt, map[string]any{"agent_id": job.ID, "text": "peer input", "mode": mode, "source": peer})
		if result.IsError || payload["mode"] != mode {
			t.Fatalf("%s prompt failed or changed timing: %v", mode, payload)
		}
		frame := fake.waitForNthCommand(t, "prompt", index+6)
		wantBehavior := mode
		if mode == "follow_up" {
			wantBehavior = "followUp"
		}
		if frame["streamingBehavior"] != wantBehavior || !reflect.DeepEqual(frame["source"], peer) {
			t.Errorf("%s changed RPC admission or presentation: %v", mode, frame)
		}
	}
}

func TestRegistryPromptSourceSurvivesRestartAndPickup(t *testing.T) {
	srv := newTestStack(t)
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "test-machine", "session_id": "source-test", "capabilities": []string{"prompt", "steer", "aside", "follow_up"}})
	peer := map[string]any{"kind": "agent", "from": "Peer", "name": "Researcher", "title": "Observed session", "route": "supervisor"}
	sources := []any{nil, peer, nil}
	modes := []string{"steer", "aside", "follow_up"}
	ids := make([]string, len(sources))
	for index, source := range sources {
		args := map[string]any{"agent_id": id, "text": "same text", "mode": modes[index], "from": "history label", "wait_seconds": 0}
		if source != nil {
			args["source"] = source
		}
		result, payload := callTool(t, handlePrompt, args)
		if result.IsError {
			t.Fatalf("prompt failed: %v", payload)
		}
		ids[index], _ = payload["delivery_id"].(string)
	}
	baseDir := filepath.Dir(globalAgents.dir)
	reloaded, err := newAgentRegistry(baseDir)
	if err != nil {
		t.Fatal(err)
	}
	restarted := httptest.NewServer(reloaded.registryMux())
	defer restarted.Close()
	code, inbox := doJSON(t, http.MethodGet, restarted.URL+"/api/agents/"+id+"/inbox?wait=0", token, nil)
	if code != http.StatusOK {
		t.Fatalf("inbox returned %d: %v", code, inbox)
	}
	items, _ := inbox["deliveries"].([]any)
	if len(items) != len(sources) {
		t.Fatalf("pending human/peer prompts were consumed or duplicated: %v", inbox)
	}
	for index, item := range items {
		delivery := item.(map[string]any)
		if delivery["id"] != ids[index] || delivery["text"] != "same text" || delivery["mode"] != modes[index] || delivery["from"] != "history label" || !reflect.DeepEqual(delivery["source"], sources[index]) {
			t.Errorf("persisted delivery %d lost identity or provenance: %v", index, delivery)
		}
	}
	_, empty := doJSON(t, http.MethodGet, restarted.URL+"/api/agents/"+id+"/inbox?wait=0", token, nil)
	if deliveries, _ := empty["deliveries"].([]any); len(deliveries) != 0 {
		t.Errorf("pickup duplicated delivered prompts: %v", empty)
	}
}

func TestRunAndScheduledSourceReachRPCChild(t *testing.T) {
	for _, launch := range []string{"run", "scheduled"} {
		for _, author := range []string{"human", "LaunchPeer"} {
			t.Run(launch+"/"+author, func(t *testing.T) {
				base := newTestStores(t)
				binary, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				t.Setenv("HOME", base)
				t.Setenv("SUPERVISOR_TEST_BINARY", binary)
				t.Setenv("SUPERVISOR_TEST_RPC_CHILD", "local")
				t.Setenv("SUPERVISOR_TEST_HOLD_RPC_CHILD", "")
				t.Setenv("SUPERVISOR_TEST_EXPECT_SOURCE", author)
				wrapper := filepath.Join(base, "omp")
				if err := os.WriteFile(wrapper, []byte("#!/bin/sh\nexec \"$SUPERVISOR_TEST_BINARY\" -test.run=^TestSupervisorRPCChildProcess$ -- \"$@\"\n"), 0700); err != nil {
					t.Fatal(err)
				}
				t.Setenv("OMP_BIN", wrapper)
				args := map[string]any{"prompt": "same assignment", "cwd": base, "from": "LaunchPeer", "steerable": author == "human"}
				if author != "human" {
					args["source"] = map[string]any{"kind": "agent", "from": author}
				}
				var jobID string
				if launch == "run" {
					result, receipt := callTool(t, handleRun, args)
					if result.IsError {
						t.Fatalf("run failed: %v", receipt)
					}
					jobID, _ = receipt["job_id"].(string)
				} else {
					target := &Job{ID: "sub_parent", PID: os.Getpid(), Status: "running", CWD: base, StartTime: time.Now()}
					globalStore.add(target)
					args["job_id"] = target.ID
					args["resume"] = false
					result, receipt := callTool(t, handleSchedule, args)
					if result.IsError {
						t.Fatalf("schedule failed: %v", receipt)
					}
					scheduleID, _ := receipt["schedule_id"].(string)
					reloaded, err := newScheduleStore(base)
					if err != nil {
						t.Fatal(err)
					}
					globalSchedules = reloaded
					reloaded.onJobTerminal(target.ID)
					sch := reloaded.get(scheduleID)
					if sch == nil || sch.Status != "started" {
						t.Fatalf("schedule did not launch after reload: %+v", sch)
					}
					jobID = sch.ChildJobID
				}
				job := globalStore.get(jobID)
				if job == nil {
					t.Fatalf("launch has no durable job: %s", jobID)
				}
				defer func() {
					job.cancelFn()
					<-job.monitorDone
				}()
				select {
				case <-job.monitorDone:
				case <-time.After(10 * time.Second):
					t.Fatal("child did not finish the attributed assignment")
				}
				job.mu.RLock()
				defer job.mu.RUnlock()
				if job.Status != "done" || job.Report != "last frame survived" || !job.Steerable {
					t.Fatalf("child rejected source or never received an RPC assignment: status=%s report=%q steerable=%v", job.Status, job.Report, job.Steerable)
				}
			})
		}
	}
}

func TestGrokWebhookIncludesOnlyCallerAgentSource(t *testing.T) {
	srv := newTestStack(t)
	ompID, ompToken := registerTestAgent(t, srv, map[string]any{
		"name": "grok-coding", "machine": "omp-host", "session_id": "sess-webhook-shadow",
	})
	unknownID, _ := registerTestAgent(t, srv, map[string]any{"name": "grok-unknown", "machine": "omp-host"})
	t.Setenv("SUPERVISOR_PUBLIC_BASE_URL", "https://supervisor.example.test")
	var mu sync.Mutex
	var posts []map[string]any
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "Bearer secret-key" {
			t.Errorf("auth header %q", auth)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("bad json: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		posts = append(posts, payload)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)

		callbackURL, _ := payload["callback_url"].(string)
		id := strings.TrimPrefix(callbackURL, "https://supervisor.example.test/api/agents/callback/")
		cbReq := httptest.NewRequest(http.MethodPost, "/api/agents/callback/"+id, bytes.NewReader([]byte(`{"ok":true,"answer":"pong"}`)))
		rr := httptest.NewRecorder()
		handleGrokCallback(rr, cbReq)
		if rr.Code != http.StatusOK {
			t.Errorf("callback status %d", rr.Code)
		}
	}))
	defer fake.Close()
	t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", fmt.Sprintf(`[{"id":"grok-coding","name":"Coding","url":%q,"key":"secret-key"}]`, fake.URL))

	for _, tc := range []struct {
		ref       string
		id        string
		source    string
		transport string
	}{
		{ref: "grok-coding", id: "grok-coding", source: "grok", transport: "webhook"},
		{ref: ompID, id: ompID, source: "registered", transport: "poll"},
		{ref: "sess-webhook-shadow", id: ompID, source: "registered", transport: "poll"},
	} {
		result, payload := callTool(t, handleAgent, map[string]any{"agent_id": tc.ref})
		row, _ := payload["agent"].(map[string]any)
		if result.IsError || row["agent_id"] != tc.id || row["source"] != tc.source || row["transport"] != tc.transport {
			t.Fatalf("registration label changed identity resolution for %q: %v", tc.ref, payload)
		}
	}

	peer := map[string]any{"kind": "agent", "from": "Peer", "name": "Researcher", "title": "Observed session", "route": "omp"}
	result, payload := callTool(t, handlePrompt, map[string]any{
		"agent_id":     "grok-coding",
		"text":         "same text",
		"from":         "history label",
		"source":       peer,
		"wait_seconds": 2,
	})
	if result.IsError || payload["agent_id"] != "grok-coding" || payload["source"] != "grok" || payload["transport"] != "webhook" {
		t.Fatalf("canonical agent prompt was shadowed by a registration name: %v", payload)
	}
	for _, absent := range []string{"mode", "start_time", "end_time", "steerable"} {
		if _, exists := payload[absent]; exists {
			t.Fatalf("one-shot webhook response claimed session timing/control: %v", payload)
		}
	}
	result, payload = callTool(t, handlePrompt, map[string]any{
		"agent_id":     "grok-coding",
		"text":         "same text",
		"from":         "history label",
		"wait_seconds": 2,
	})
	if result.IsError || payload["agent_id"] != "grok-coding" || payload["transport"] != "webhook" {
		t.Fatalf("canonical human prompt was shadowed by a registration name: %v", payload)
	}
	result, payload = callTool(t, handlePrompt, map[string]any{
		"agent_id":     "grok-coding",
		"text":         "same text",
		"from":         "history label",
		"source":       map[string]any{"kind": "agent", "from": " "},
		"wait_seconds": 2,
	})
	if !result.IsError {
		t.Fatal("invalid source was forwarded")
	}
	for _, ref := range []string{"coding", "Coding", "cos", "Chief of Staff", "grok-unknown", "grok-cod"} {
		if result, payload := callTool(t, handlePrompt, map[string]any{"agent_id": ref, "text": "must not reach webhook", "wait_seconds": 1}); !result.IsError {
			t.Errorf("generic alias dispatched a webhook: %q: %v", ref, payload)
		}
	}
	for _, mode := range []string{"steer", "aside", "follow_up"} {
		if result, payload := callTool(t, handleSteer, map[string]any{"agent_id": "grok-coding", "text": "must not reach webhook", "mode": mode}); !result.IsError {
			t.Errorf("steer/message invoked one-shot webhook in %s mode: %v", mode, payload)
		}
	}
	if ag := globalAgents.get(unknownID); ag == nil || len(ag.Queue) != 0 || len(ag.History) != 0 {
		t.Fatalf("unknown canonical route fell through to a registration label: %#v", ag)
	}
	result, payload = callTool(t, handlePrompt, map[string]any{
		"agent_id": ompID, "text": "OMP input", "wait_seconds": 0,
	})
	if result.IsError || payload["agent_id"] != ompID || payload["source"] != "registered" || payload["transport"] != "poll" {
		t.Fatalf("webhook-looking name stole the actual OMP ID: %v", payload)
	}
	code, inbox := doJSON(t, http.MethodGet, srv.URL+"/api/agents/"+ompID+"/inbox?wait=0", ompToken, nil)
	deliveries, _ := inbox["deliveries"].([]any)
	if code != http.StatusOK || len(deliveries) != 1 {
		t.Fatalf("canonical webhook prompts leaked into the OMP queue: %v", inbox)
	}
	if delivery := deliveries[0].(map[string]any); delivery["text"] != "OMP input" {
		t.Fatalf("wrong target reached the registered OMP bridge: %v", delivery)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(posts) != 2 {
		t.Fatalf("webhook posts = %d, want 2 (invalid source must not be sent): %#v", len(posts), posts)
	}
	if posts[0]["prompt"] != "same text" || posts[0]["from"] != "history label" || posts[0]["source"] != "supervisor" || !reflect.DeepEqual(posts[0]["prompt_source"], peer) {
		t.Fatalf("agent ask lost provenance or event origin: %#v", posts[0])
	}
	if _, ok := posts[1]["prompt_source"]; ok || posts[1]["from"] != "history label" || posts[1]["source"] != "supervisor" || posts[1]["prompt"] != "same text" {
		t.Fatalf("human ask invented a source: %#v", posts[1])
	}
}

func TestPromptPresentationDoesNotGrantAuthorshipOrChangeRouting(t *testing.T) {
	srv := newTestStack(t)
	id, _ := registerTestAgent(t, srv, map[string]any{"machine": "omp-host", "capabilities": []string{"prompt", "steer", "aside", "follow_up"}})
	for _, source := range []map[string]any{
		{"name": "Researcher", "title": "Task", "route": "grok-webhook"},
		{"kind": "agent", "from": "Peer", "name": false},
		{"kind": "agent", "from": "Peer", "title": 42},
		{"kind": "agent", "from": "Peer", "route": "coding"},
	} {
		result, payload := callTool(t, handlePrompt, map[string]any{"agent_id": id, "text": "not admitted", "source": source, "wait_seconds": 0})
		if !result.IsError {
			t.Fatalf("invalid provenance/presentation admitted: %v", payload)
		}
	}
	ag := globalAgents.get(id)
	if ag == nil || len(ag.Queue) != 0 {
		t.Fatalf("invalid source changed delivery state: %#v", ag)
	}
	source := map[string]any{"kind": "agent", "from": "Peer", "name": "Webhook-looking label", "route": "grok-webhook"}
	result, payload := callTool(t, handlePrompt, map[string]any{"agent_id": id, "text": "OMP input", "source": source, "mode": "aside", "wait_seconds": 0})
	if result.IsError || payload["agent_id"] != id || payload["transport"] != "poll" || payload["mode"] != "aside" {
		t.Fatalf("presentation changed routing or admission: %v", payload)
	}
}
