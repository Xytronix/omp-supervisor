package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

func TestEventsCursorFiltersControlFramesAndRetainsPartialTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "child.log")
	ready := "{\"type\":\"ready\"}\n"
	first := "{\"type\":\"message_update\",\"assistantMessageEvent\":{\"type\":\"text_delta\",\"delta\":\"héllo\"}}\r\n"
	controls := "{\"type\":\"response\",\"command\":\"get_state\",\"success\":true}\n" +
		"{\"type\":\"extension_ui_request\",\"method\":\"confirm\"}\n" +
		"{\"type\":\"model_change\",\"model\":\"journal/not-rpc\"}\n" +
		"{\"type\":\"thinking_level_change\",\"thinkingLevel\":\"max\"}\n" +
		"{\"type\":\"steer\",\"message\":\"not an event\"}\ninvalid JSON\n"
	second := "{\"type\":\"tool_execution_end\",\"toolCallId\":\"call_1\",\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"done\"}]}}\n"
	partial := "{\"type\":\"agent_end\",\"messages\":[]}"
	complete := ready + first + controls + second
	if err := os.WriteFile(path, []byte(complete+partial), 0600); err != nil {
		t.Fatal(err)
	}
	page, err := readJobEvents(context.Background(), path, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	firstCursor := int64(len(ready + first))
	if page.Reset || !page.HasMore || page.NextCursor != firstCursor || len(page.Events) != 1 || page.Events[0].Cursor != firstCursor {
		t.Fatalf("first page skipped/repeated bytes: %+v", page)
	}
	if string(page.Events[0].Event) != strings.TrimSpace(first) {
		t.Fatalf("raw delta payload changed: %s", page.Events[0].Event)
	}
	page, err = readJobEvents(context.Background(), path, page.NextCursor, 100)
	if err != nil {
		t.Fatal(err)
	}
	if page.FromCursor != firstCursor || page.NextCursor != int64(len(complete)) || page.HasMore || len(page.Events) != 1 || string(page.Events[0].Event) != strings.TrimSpace(second) {
		t.Fatalf("control frames leaked or partial frame consumed: %+v", page)
	}
	atTail, err := readJobEvents(context.Background(), path, page.NextCursor, 100)
	if err != nil || atTail.NextCursor != page.NextCursor || atTail.HasMore || len(atTail.Events) != 0 {
		t.Fatalf("partial tail advanced: %+v, %v", atTail, err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("\n"); err != nil {
		t.Fatal(err)
	}
	file.Close()
	finished, err := readJobEvents(context.Background(), path, page.NextCursor, 100)
	if err != nil || len(finished.Events) != 1 || finished.NextCursor != int64(len(complete+partial)+1) || string(finished.Events[0].Event) != partial {
		t.Fatalf("completed tail lost or duplicated: %+v, %v", finished, err)
	}
}

func TestEventsResetInvalidBoundaryAndTruncatedLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "child.log")
	frame := "{\"type\":\"agent_start\"}\n"
	if err := os.WriteFile(path, []byte(frame+frame), 0600); err != nil {
		t.Fatal(err)
	}
	inside, err := readJobEvents(context.Background(), path, 5, 1)
	if err != nil || !inside.Reset || inside.FromCursor != 0 || inside.NextCursor != int64(len(frame)) || len(inside.Events) != 1 {
		t.Fatalf("mid-frame cursor not reset: %+v, %v", inside, err)
	}
	oldCursor := int64(2 * len(frame))
	if err := os.WriteFile(path, []byte(frame), 0600); err != nil {
		t.Fatal(err)
	}
	reset, err := readJobEvents(context.Background(), path, oldCursor, 100)
	if err != nil || !reset.Reset || reset.FromCursor != 0 || reset.NextCursor != int64(len(frame)) || len(reset.Events) != 1 {
		t.Fatalf("truncated log not replayed: %+v, %v", reset, err)
	}
}

func TestRPCReadLoopDoesNotCompleteAnUnterminatedEvent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "child.log")
	logFile, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	job := &Job{ID: "sub_partial", Status: "running"}
	child := &rpcChild{job: job, logFile: logFile, terminal: make(chan struct{}, 1)}
	complete := "{\"type\":\"agent_start\"}\n"
	partial := "{\"type\":\"agent_end\",\"messages\":[]}"
	child.readLoop(io.NopCloser(strings.NewReader(complete + partial)))
	page, err := readJobEvents(context.Background(), path, 0, 100)
	if err != nil || page.NextCursor != int64(len(complete)) || len(page.Events) != 1 {
		t.Fatalf("capture acknowledged an unterminated event: %+v, %v", page, err)
	}

	if _, err := logFile.WriteString("\n"); err != nil {
		t.Fatal(err)
	}
	page, err = readJobEvents(context.Background(), path, page.NextCursor, 100)
	if err != nil || len(page.Events) != 1 || string(page.Events[0].Event) != partial {
		t.Fatalf("partial capture was changed: %+v, %v", page, err)
	}
}

func TestEventsRestartRecoversObservedMetadataWithoutInventingCompletion(t *testing.T) {
	oldStore := globalStore
	base := t.TempDir()
	store, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = oldStore })
	path := filepath.Join(store.logsDir, "sub_recovery.log")
	job := &Job{
		ID: "sub_recovery", Status: "running", PID: 0, Steerable: true,
		Machine: "remote-test", LogPath: path, ModelRequested: "provider/requested:high",

		ModelResolved: "provider/requested", Thinking: "high",
		ParentSessionID: "parent", RootSessionID: "root", ParentAgentID: "agent", ParentToolCallID: "call",
		Title:     "Launch display name",
		StartTime: time.Now(),
	}
	store.add(job)
	state := "{\"type\":\"response\",\"command\":\"get_state\",\"success\":true,\"data\":{\"model\":{\"provider\":\"actual\",\"id\":\"chosen\"},\"thinkingLevel\":\"low\",\"sessionId\":\"child-session\",\"sessionName\":\"Observed session title\"}}\n"
	message := "{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"recovered answer\"}]}}\n"

	failed := "{\"type\":\"response\",\"command\":\"set_model\",\"success\":false,\"data\":{\"model\":{\"provider\":\"bad\",\"id\":\"wrong\"}}}\n"
	end := "{\"type\":\"agent_end\",\"messages\":[]}\n"
	partial := "{\"type\":\"thinking_level_changed\",\"thinkingLevel\":\"max\"}"
	if err := os.WriteFile(path, []byte(state+message+failed+end+partial), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := readJobEvents(context.Background(), path, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	globalStore = reloaded
	res, payload := callTool(t, handleEvents, map[string]any{"job_id": job.ID, "after": before.NextCursor})
	if res.IsError || payload["stream_closed"] != true || payload["status"] != "error" || payload["machine"] != "remote-test" || payload["session_id"] != "child-session" || payload["model_resolved"] != "actual/chosen" || payload["thinking"] != "low" {
		t.Fatalf("restart lost observation or invented success: %v", payload)
	}
	if payload["title"] != "Launch display name" || payload["session_title"] != "Observed session title" || payload["start_time"] != job.StartTime.Format(time.RFC3339Nano) || payload["end_time"] == nil {
		t.Fatalf("restart lost distinct observed title or durable execution timing: %v", payload)
	}
	for key, want := range map[string]string{"parent_session_id": "parent", "root_session_id": "root", "parent_agent_id": "agent", "parent_tool_call_id": "call", "model_requested": "provider/requested:high"} {
		if payload[key] != want {
			t.Fatalf("restart lost %s: %v", key, payload)
		}
	}
	var page jobEventPage
	data, err := json.Marshal(payload)
	if err != nil || json.Unmarshal(data, &page) != nil {
		t.Fatal("invalid event page")
	}
	if page.Reset || len(page.Events) != 1 || page.NextCursor != int64(len(state+message+failed+end)) {
		t.Fatalf("restart cursor replayed or skipped events: %+v", page)
	}
	var event rpcFrame
	if err := json.Unmarshal(page.Events[0].Event, &event); err != nil || event.Type != "agent_end" {
		t.Fatalf("restart returned the wrong child event: %s, %v", page.Events[0].Event, err)
	}
	_, result := callTool(t, handleResult, map[string]any{"job_id": job.ID})
	if result["report"] != "recovered answer" || result["thinking"] != "low" || result["parent_session_id"] != "parent" || result["session_title"] != "Observed session title" || result["title"] != "Launch display name" {
		t.Fatalf("restart result lost transcript/metadata: %v", result)
	}

	globalStore, err = newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	_, again := callTool(t, handleEvents, map[string]any{"job_id": job.ID, "after": before.NextCursor})
	if !reflect.DeepEqual(payload, again) {
		t.Fatalf("cursor not stable across restart: %v != %v", payload, again)
	}
}

func TestEventsRejectUnavailableLogsAndInvalidCursors(t *testing.T) {
	oldStore := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = oldStore })
	store.add(&Job{ID: "sub_print", Status: "done"})
	store.add(&Job{ID: "sub_missing", Status: "done", Steerable: true, LogPath: filepath.Join(store.logsDir, "missing.log")})
	for _, id := range []string{"unknown", "sub_print", "sub_missing"} {
		res, payload := callTool(t, handleEvents, map[string]any{"job_id": id})
		if !res.IsError {
			t.Fatalf("unavailable stream became a completed empty page: %v", payload)
		}
	}
	path := filepath.Join(store.logsDir, "valid.log")
	if err := os.WriteFile(path, []byte("{\"type\":\"agent_start\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	store.add(&Job{ID: "sub_valid", Steerable: true, LogPath: path})
	for _, after := range []any{-1, 0.5, "0", float64(9007199254740992)} {
		res, payload := callTool(t, handleEvents, map[string]any{"job_id": "sub_valid", "after": after})
		if !res.IsError {
			t.Fatalf("invalid cursor %v accepted: %v", after, payload)
		}
	}
}

func TestObservedModelChangesInvalidateOldEffortAndSurviveRestart(t *testing.T) {
	oldStore := globalStore
	base := t.TempDir()
	store, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = oldStore })
	job := &Job{ID: "sub_metadata", Machine: "remote-test", Status: "done", Model: "requested/model:high", ModelRequested: "requested/model:high", MetadataVersion: 1}
	store.add(job)
	_, before := callTool(t, handleStatus, map[string]any{"job_id": job.ID})
	if _, ok := before["model_resolved"]; ok {
		t.Fatalf("unobserved model fabricated from request: %v", before)
	}
	if _, ok := before["thinking"]; ok {
		t.Fatalf("unobserved effort fabricated from request: %v", before)
	}
	child := &rpcChild{job: job, store: store}
	succeeded := true
	child.handleFrame(rpcFrame{Type: "response", Command: "get_state", Success: &succeeded, Data: json.RawMessage(`{"model":{"provider":"actual","id":"first"},"thinkingLevel":"low"}`)})
	child.handleFrame(rpcFrame{Type: "model_changed"})
	_, switched := callTool(t, handleStatus, map[string]any{"job_id": job.ID})
	if _, ok := switched["model_resolved"]; ok {
		t.Fatalf("payload-free model change retained a stale model: %v", switched)
	}
	if _, ok := switched["thinking"]; ok {
		t.Fatalf("old model effort survived a model switch: %v", switched)
	}
	child.handleFrame(rpcFrame{Type: "response", Command: "set_model", Success: &succeeded, Data: json.RawMessage(`{"provider":"actual","id":"second"}`)})
	child.handleFrame(rpcFrame{Type: "thinking_level_changed", ThinkingLevel: "off"})

	globalStore, err = newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	_, recovered := callTool(t, handleStatus, map[string]any{"job_id": job.ID})
	if recovered["model_resolved"] != "actual/second" || recovered["thinking"] != "off" || recovered["model_requested"] != "requested/model:high" {
		t.Fatalf("durable observation lost: %v", recovered)
	}
}

func TestSessionTitleObservationClearsOnlyOnExplicitStateAndSurvivesRestart(t *testing.T) {
	oldStore := globalStore
	base := t.TempDir()
	t.Setenv("HOME", base)
	store, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = oldStore })
	start := time.Date(2026, 10, 17, 8, 0, 0, 0, time.UTC)
	end := start.Add(3 * time.Minute)
	job := &Job{ID: "sub_title", Status: "done", Title: "Launch name", StartTime: start, EndTime: end, MetadataVersion: metadataVersion}
	store.add(job)
	_, before := callTool(t, handleStatus, map[string]any{"job_id": job.ID})
	if before["session_title"] != "" {
		t.Fatalf("launch name was promoted to an observed session title: %v", before)
	}
	child := &rpcChild{job: job, store: store}
	succeeded, failed := true, false
	child.handleFrame(rpcFrame{Type: "response", Command: "get_state", Success: &succeeded, Data: json.RawMessage(`{"sessionName":"Observed title"}`)})
	child.handleFrame(rpcFrame{Type: "response", Command: "get_state", Success: &failed, Data: json.RawMessage(`{"sessionName":"Failed observation"}`)})
	child.handleFrame(rpcFrame{Type: "response", Command: "get_state", Success: &succeeded, Data: json.RawMessage(`{}`)})
	globalStore, err = newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	reloaded := globalStore.get(job.ID)
	if reloaded == nil || reloaded.Title != "Launch name" || reloaded.SessionTitle != "Observed title" || !reloaded.StartTime.Equal(start) || !reloaded.EndTime.Equal(end) {
		t.Fatalf("title/timing observation was synthesized or lost at restart: %#v", reloaded)
	}
	for _, handler := range []func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error){handleStatus, handleResult} {
		result, payload := callTool(t, handler, map[string]any{"job_id": job.ID})
		if result.IsError || payload["title"] != "Launch name" || payload["session_title"] != "Observed title" || payload["start_time"] != start.Format(time.RFC3339) || payload["end_time"] != end.Format(time.RFC3339) {
			t.Fatalf("explicit read lost observed title/timing: %v", payload)
		}
	}
	child = &rpcChild{job: reloaded, store: globalStore}
	child.handleFrame(rpcFrame{Type: "response", Command: "get_state", Success: &succeeded, Data: json.RawMessage(`{"sessionName":""}`)})
	_, cleared := callTool(t, handleStatus, map[string]any{"job_id": job.ID})
	if cleared["session_title"] != "" || reloaded.Title != "Launch name" || cleared["elapsed_s"] != float64(180) {
		t.Fatalf("explicit title clearing changed launch name or terminal duration: %v", cleared)
	}
	globalStore, err = newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	_, restored := callTool(t, handleResult, map[string]any{"job_id": job.ID})
	if restored["session_title"] != "" || restored["title"] != "Launch name" {
		t.Fatalf("cleared title was not durable across restart: %v", restored)
	}
}

func TestStatusDoesNotInventUnknownOrAdvancingTerminalDuration(t *testing.T) {
	base := newTestStores(t)
	t.Setenv("HOME", base)
	for _, job := range []*Job{
		{ID: "sub_unknown_start", Status: "done"},
		{ID: "sub_unknown_end", Status: "done", StartTime: time.Date(2026, 10, 17, 8, 0, 0, 0, time.UTC)},
	} {
		globalStore.add(job)
		_, payload := callTool(t, handleStatus, map[string]any{"job_id": job.ID})
		if _, exists := payload["elapsed_s"]; exists {
			t.Fatalf("unknown timing became a growing execution duration: %v", payload)
		}
		if job.StartTime.IsZero() {
			if _, exists := payload["start_time"]; exists {
				t.Fatalf("unobserved start timestamp published: %v", payload)
			}
		}
	}
}

func TestEventsLimitBoundsRetainNextPage(t *testing.T) {
	oldStore := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = oldStore })
	path := filepath.Join(store.logsDir, "bounded.log")
	frame := "{\"type\":\"turn_start\"}\n"
	if err := os.WriteFile(path, []byte(strings.Repeat(frame, 502)), 0600); err != nil {
		t.Fatal(err)
	}
	store.add(&Job{ID: "sub_bounded", Steerable: true, LogPath: path})
	res, page := callTool(t, handleEvents, map[string]any{"job_id": "sub_bounded", "limit": 1000})
	events, _ := page["events"].([]any)
	if res.IsError || len(events) != 500 || page["next_cursor"] != float64(500*len(frame)) || page["has_more"] != true {
		t.Fatalf("event page exceeded limit or lost cursor: %v", page)
	}
	res, next := callTool(t, handleEvents, map[string]any{"job_id": "sub_bounded", "after": page["next_cursor"], "limit": 0})
	events, _ = next["events"].([]any)
	if res.IsError || len(events) != 1 || next["next_cursor"] != float64(501*len(frame)) || next["has_more"] != true {
		t.Fatalf("minimum limit skipped/repeated remaining frames: %v", next)
	}
}

func TestRPCPollingUsesCapturedStateAndFinalizesFallbackOnce(t *testing.T) {
	oldStore := globalStore
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("SUPERVISOR_MACHINE", "polling-test-host")
	store, err := newJobStore(filepath.Join(home, "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = oldStore })
	sessionDir := filepath.Join(home, ".omp", "agent", "sessions", "-")
	if err := os.MkdirAll(sessionDir, 0700); err != nil {
		t.Fatal(err)
	}
	sessionPath := filepath.Join(sessionDir, "time_child-session.jsonl")
	session := "{\"type\":\"session\",\"id\":\"child-session\"}\n" +
		"{\"type\":\"custom\",\"customType\":\"tool_execution_start\",\"data\":{\"toolName\":\"write\",\"args\":{\"path\":\"finished.go\"}}}\n" +
		"{\"type\":\"message\",\"message\":{\"role\":\"toolResult\",\"toolName\":\"todo\",\"content\":[{\"type\":\"text\",\"text\":\"Overall: 2/2 done, 0 open\"}]}}\n"
	if err := os.WriteFile(sessionPath, []byte(session), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "REPORT.md"), []byte("report from another run"), 0600); err != nil {
		t.Fatal(err)
	}
	job := &Job{
		ID: "sub_polling", Machine: "polling-test-host", Status: "running", Steerable: true,
		CWD: home, SessionID: "child-session", ModelResolved: "actual/model", Thinking: "low",
		MetadataVersion: 1, LastTool: "read", StartTime: time.Now(),
	}
	store.add(job)
	_, live := callTool(t, handleStatus, map[string]any{"job_id": job.ID})
	if live["last_tool"] != "read" || live["todos_done"] != float64(0) {
		t.Fatalf("live polling replaced RPC state with session fallback: %v", live)
	}
	_, pending := callTool(t, handleResult, map[string]any{"job_id": job.ID})
	if pending["report"] != "" {
		t.Fatalf("live job without an assistant report used disk fallback: %v", pending)
	}
	job.mu.Lock()
	job.Report = "captured answer"
	job.mu.Unlock()
	_, result := callTool(t, handleResult, map[string]any{"job_id": job.ID})
	if result["report"] != "captured answer" {
		t.Fatalf("RPC result used a shared report instead of captured text: %v", result)
	}
	job.mu.Lock()
	job.Status = "done"
	job.mu.Unlock()
	_, final := callTool(t, handleStatus, map[string]any{"job_id": job.ID})
	if final["todos_done"] != float64(2) {
		t.Fatalf("terminal enrichment was not captured: %v", final)
	}

	store.save(job)
	resumed := "{\"type\":\"custom\",\"customType\":\"tool_execution_start\",\"data\":{\"toolName\":\"write\",\"args\":{\"path\":\"later-job.go\"}}}\n" +
		"{\"type\":\"message\",\"message\":{\"role\":\"toolResult\",\"toolName\":\"todo\",\"content\":[{\"type\":\"text\",\"text\":\"Overall: 9/10 done, 1 open\"}]}}\n"
	if err := os.WriteFile(sessionPath, []byte(session+resumed), 0600); err != nil {
		t.Fatal(err)
	}
	_, again := callTool(t, handleStatus, map[string]any{"job_id": job.ID})
	if again["todos_done"] != float64(2) || again["todos_open"] != float64(0) {
		t.Fatalf("repeated polling re-read a resumed session: %v", again)
	}
	globalStore, err = newJobStore(filepath.Join(home, "supervisor"))
	if err != nil {
		t.Fatal(err)
	}
	_, recovered := callTool(t, handleResult, map[string]any{"job_id": job.ID})
	if recovered["report"] != "captured answer" {
		t.Fatalf("restart lost the captured report: %v", recovered)
	}
	files, _ := recovered["files_changed"].([]any)
	foundFinished := false
	for _, file := range files {
		if file == "later-job.go" {
			t.Fatalf("restart re-scanned another job's session: %v", recovered)
		}
		if file == "finished.go" {
			foundFinished = true
		}
	}
	if !foundFinished {
		t.Fatalf("restart lost the final metadata snapshot: %v", recovered)
	}
}

func TestObservedModelThinkingUsesEffectiveAutoEffort(t *testing.T) {
	job := &Job{ModelResolved: "actual/model", Thinking: "high"}
	for _, step := range []struct {
		frame string
		want  string
	}{
		{`{"type":"thinking_level_changed","thinkingLevel":"low","configured":"auto"}`, "low"},
		{`{"type":"thinking_level_changed","thinkingLevel":"high","configured":"auto","resolved":"high"}`, "high"},
		{`{"type":"thinking_level_changed","thinkingLevel":"auto","resolved":"medium"}`, "medium"},
		{`{"type":"thinking_level_changed","configured":"auto"}`, ""},
		{`{"type":"thinking_level_changed","thinkingLevel":"auto"}`, ""},
	} {
		frame, err := decodeFrame([]byte(step.frame))
		if err != nil {
			t.Fatal(err)
		}
		job.observeExecutionLocked(frame)
		metadata := make(map[string]any)
		job.addExecutionMetadataLocked(metadata)
		if step.want == "" {
			if _, ok := metadata["thinking"]; ok {
				t.Fatalf("unresolved effort was advertised as actual: %v", metadata)
			}
		} else if metadata["thinking"] != step.want {
			t.Fatalf("wrong effective effort after %s: %v", step.frame, metadata)
		}
	}
}

type rpcWriteNotice struct {
	io.WriteCloser
	started chan struct{}
	once    sync.Once
}

func (w *rpcWriteNotice) Write(data []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	return w.WriteCloser.Write(data)
}

func TestRPCReadLoopRefreshesModelWithoutBlockingOutput(t *testing.T) {
	oldStore := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = oldStore })
	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()
	logFile, err := os.Create(filepath.Join(store.logsDir, "switch.log"))
	if err != nil {
		t.Fatal(err)
	}
	job := &Job{
		ID: "sub_switch", Machine: "remote-test", Status: "running", Steerable: true,
		ModelResolved: "old/model", Thinking: "high", ModelRequested: "requested/model",
		LogPath: logFile.Name(), captureDone: make(chan struct{}),
	}
	store.add(job)
	before, after := make(chan rpcFrame, 1), make(chan rpcFrame, 1)
	input := &rpcWriteNotice{WriteCloser: stdinWriter, started: make(chan struct{})}
	child := &rpcChild{
		job: job, store: store, stdin: input, logFile: logFile,
		pending: map[string]chan rpcFrame{"before": before, "after": after},
		replies: make(chan []byte, 64), stateRefresh: make(chan struct{}, 1),
		done: make(chan struct{}), readDone: make(chan struct{}),
	}
	writerDone := make(chan struct{})
	go func() { child.replyLoop(); close(writerDone) }()
	go child.readLoop(stdoutReader)
	t.Cleanup(func() {
		stdinReader.Close()
		child.close()
		stdoutWriter.Close()
		select {
		case <-child.readDone:
		case <-time.After(5 * time.Second):
			t.Error("RPC reader did not stop")
		}
		select {
		case <-writerDone:
		case <-time.After(5 * time.Second):
			t.Error("RPC writer did not stop")
		}
		logFile.Close()
	})
	if _, err := io.WriteString(stdoutWriter, "{\"type\":\"model_changed\"}\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-input.started:
	case <-time.After(5 * time.Second):
		t.Fatal("model change never scheduled a state refresh")
	}
	sent := make(chan error, 1)
	go func() {
		_, err := io.WriteString(stdoutWriter, "{\"type\":\"tool_execution_start\",\"toolName\":\"read\"}\n{\"type\":\"response\",\"id\":\"before\",\"command\":\"barrier\",\"success\":true}\n")
		sent <- err
	}()

	select {
	case <-before:
	case <-time.After(5 * time.Second):
		t.Fatal("model refresh blocked the stdout reader")
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	_, invalidated := callTool(t, handleStatus, map[string]any{"job_id": job.ID})
	if _, ok := invalidated["model_resolved"]; ok {
		t.Fatalf("old model survived model_changed: %v", invalidated)
	}
	if invalidated["last_tool"] != "read" {
		t.Fatalf("subsequent output was not consumed: %v", invalidated)
	}
	queried := make(chan error, 1)
	go func() {
		commands := newRPCLogScanner(stdinReader)
		if !commands.Scan() {
			queried <- io.ErrUnexpectedEOF
			return
		}
		var command rpcFrame
		if err := json.Unmarshal(commands.Bytes(), &command); err != nil {
			queried <- err
		} else if command.Type != "get_state" {
			queried <- io.ErrUnexpectedEOF
		} else {
			queried <- nil
		}
	}()
	select {
	case err := <-queried:
		if err != nil {
			t.Fatalf("queued refresh was not a state query: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("queued refresh never reached stdin")
	}
	_, err = io.WriteString(stdoutWriter, "{\"type\":\"response\",\"command\":\"get_state\",\"success\":true,\"data\":{\"model\":{\"provider\":\"actual\",\"id\":\"new\"},\"thinkingLevel\":\"low\",\"sessionId\":\"observed\"}}\n{\"type\":\"thinking_level_changed\",\"thinkingLevel\":\"high\",\"configured\":\"auto\",\"resolved\":\"high\"}\n{\"type\":\"response\",\"id\":\"after\",\"command\":\"barrier\",\"success\":true}\n")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-after:
	case <-time.After(5 * time.Second):
		t.Fatal("state query response was not consumed")
	}
	_, observed := callTool(t, handleStatus, map[string]any{"job_id": job.ID})
	if observed["model_resolved"] != "actual/new" || observed["thinking"] != "high" || observed["session_id"] != "observed" {
		t.Fatalf("refresh did not recover actual execution: %v", observed)
	}
	stdoutWriter.Close()
	<-child.readDone
	res, page := callTool(t, handleEvents, map[string]any{"job_id": job.ID})
	if res.IsError || page["stream_closed"] != true {
		t.Fatalf("drained capture was not closed: %v", page)
	}
	var kinds []string
	for _, raw := range page["events"].([]any) {
		kinds = append(kinds, raw.(map[string]any)["event"].(map[string]any)["type"].(string))
	}
	if strings.Join(kinds, ",") != "model_changed,tool_execution_start,thinking_level_changed" {
		t.Fatalf("RPC notifications lost or control frames exposed: %v", kinds)
	}
}

type eventScanContext struct {
	context.Context
	once       sync.Once
	duringScan func()
}

func (c *eventScanContext) Err() error {
	c.once.Do(c.duringScan)
	return c.Context.Err()
}

func TestEventsSnapshotPrecedesFinalAppendAndCaptureDrain(t *testing.T) {
	oldStore := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = oldStore })
	path := filepath.Join(store.logsDir, "drain.log")
	first, last := "{\"type\":\"agent_start\"}\n", "{\"type\":\"agent_end\",\"messages\":[]}\n"
	if err := os.WriteFile(path, []byte(first), 0600); err != nil {
		t.Fatal(err)
	}
	job := &Job{ID: "sub_drain", Status: "running", Steerable: true, LogPath: path, captureDone: make(chan struct{})}
	store.add(job)
	ctx := &eventScanContext{Context: context.Background(), duringScan: func() {
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteString(last); err != nil {
			t.Fatal(err)
		}
		file.Close()
		job.mu.Lock()
		job.Status = "done"
		job.mu.Unlock()
		close(job.captureDone)
	}}
	request := mcp.CallToolRequest{}
	request.Params.Arguments = map[string]any{"job_id": job.ID}
	result, err := handleEvents(ctx, request)
	if err != nil || result.IsError {
		t.Fatalf("first poll failed: %v, %v", result, err)
	}
	var page map[string]any
	if err := json.Unmarshal([]byte(result.Content[0].(mcp.TextContent).Text), &page); err != nil {
		t.Fatal(err)
	}
	if page["status"] != "running" || page["stream_closed"] != false || page["has_more"] != false || page["next_cursor"] != float64(len(first)) {
		t.Fatalf("an old log snapshot claimed final completion: %v", page)
	}
	_, final := callTool(t, handleEvents, map[string]any{"job_id": job.ID, "after": page["next_cursor"]})
	if final["status"] != "done" || final["stream_closed"] != true || final["has_more"] != false || final["next_cursor"] != float64(len(first+last)) {
		t.Fatalf("final append was lost after draining: %v", final)
	}
	events := final["events"].([]any)
	if len(events) != 1 || events[0].(map[string]any)["event"].(map[string]any)["type"] != "agent_end" {
		t.Fatalf("missing final event: %v", final)
	}
}

func TestEventsTerminalStatusWaitsForStdoutDrain(t *testing.T) {
	oldStore := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = oldStore })
	logFile, err := os.Create(filepath.Join(store.logsDir, "late.log"))
	if err != nil {
		t.Fatal(err)
	}
	reader, writer := io.Pipe()
	job := &Job{ID: "sub_late", Status: "done", Steerable: true, LogPath: logFile.Name(), captureDone: make(chan struct{})}
	store.add(job)
	child := &rpcChild{job: job, store: store, logFile: logFile, readDone: make(chan struct{})}
	go child.readLoop(reader)
	t.Cleanup(func() {
		writer.Close()
		<-child.readDone
		logFile.Close()
	})
	_, early := callTool(t, handleEvents, map[string]any{"job_id": job.ID})
	if early["status"] != "done" || early["has_more"] != false || early["stream_closed"] != false {
		t.Fatalf("terminal status closed a still-active capture: %v", early)
	}
	if _, err := io.WriteString(writer, "{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"late answer\"}]}}\n"); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	<-child.readDone
	_, final := callTool(t, handleEvents, map[string]any{"job_id": job.ID, "after": early["next_cursor"]})
	events := final["events"].([]any)
	if final["stream_closed"] != true || final["has_more"] != false || len(events) != 1 || events[0].(map[string]any)["event"].(map[string]any)["type"] != "message_end" {
		t.Fatalf("late output was dropped by terminal polling: %v", final)
	}
}

func TestEventsRecoveredProcessExitReconcilesOnRead(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error)
	}{
		{"status", handleStatus},
		{"result", handleResult},
		{"events", handleEvents},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldStore := globalStore
			home := t.TempDir()
			t.Setenv("HOME", home)
			t.Setenv("SUPERVISOR_MACHINE", "recovery-test-host")
			base := filepath.Join(home, "supervisor")
			store, err := newJobStore(base)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { globalStore = oldStore })

			child := exec.Command("sh", "-c", "read ignored; exit 0")
			stdin, err := child.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := child.Start(); err != nil {
				stdin.Close()
				t.Fatal(err)
			}
			waited := false
			t.Cleanup(func() {
				stdin.Close()
				if !waited {
					child.Wait()
				}
			})
			id := "sub_recovered_exit_" + tc.name
			logPath := filepath.Join(store.logsDir, id+".log")
			if err := os.WriteFile(logPath, []byte("{\"type\":\"agent_end\",\"messages\":[]}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			sessionDir := filepath.Join(home, ".omp", "agent", "sessions", "-")
			if err := os.MkdirAll(sessionDir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(sessionDir, "time_recovered-child.jsonl"), []byte("{\"type\":\"custom\",\"customType\":\"tool_execution_start\",\"data\":{\"toolName\":\"write\",\"args\":{\"path\":\"another-run.go\"}}}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			store.add(&Job{
				ID: id, PID: child.Process.Pid, Status: "running", Steerable: true,
				Machine: "recovery-test-host", CWD: home, SessionID: "recovered-child",
				ModelResolved: "actual/model", Thinking: "low", MetadataVersion: 1,
				LastTool: "read", Report: "captured answer", LogPath: logPath,
				StartTime: time.Now(), cmd: child,
			})
			globalStore, err = newJobStore(base)
			if err != nil {
				t.Fatal(err)
			}
			res, live := callTool(t, tc.handler, map[string]any{"job_id": id})
			if res.IsError || live["status"] != "running" {
				t.Fatalf("live recovered PID was treated as terminal: %v", live)
			}
			_, status := callTool(t, handleStatus, map[string]any{"job_id": id})
			_, stream := callTool(t, handleEvents, map[string]any{"job_id": id})
			if status["steerable"] != false || stream["stream_closed"] != true || stream["status"] != "running" {
				t.Fatalf("restart pretended to reattach a control/capture pipe: status=%v stream=%v", status, stream)
			}
			if err := stdin.Close(); err != nil {
				t.Fatal(err)
			}
			err = child.Wait()
			waited = true
			if err != nil {
				t.Fatalf("reading recovered status killed the live child: %v", err)
			}
			res, exited := callTool(t, tc.handler, map[string]any{"job_id": id})
			if res.IsError || exited["status"] != "error" {
				t.Fatalf("post-restart process exit stayed running or invented success: %v", exited)
			}
			path := filepath.Join(store.dataDir, id+".json")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var saved Job
			if err := json.Unmarshal(data, &saved); err != nil {
				t.Fatal(err)
			}
			if saved.Status != "error" || saved.ExitCode != -1 || saved.EndTime.IsZero() || saved.Report != "captured answer" {
				t.Fatalf("interrupted transition was not durably recorded: status=%s exit=%d end=%v report=%q", saved.Status, saved.ExitCode, saved.EndTime, saved.Report)
			}
			_, status = callTool(t, handleStatus, map[string]any{"job_id": id})
			if status["last_tool"] != "read" || status["model_resolved"] != "actual/model" {
				t.Fatalf("reconciliation scanned a resumed session instead of retaining captured metadata: %v", status)
			}
			globalStore, err = newJobStore(base)
			if err != nil {
				t.Fatal(err)
			}
			_, result := callTool(t, handleResult, map[string]any{"job_id": id})
			_, stream = callTool(t, handleEvents, map[string]any{"job_id": id})
			if result["status"] != "error" || result["exit_code"] != float64(-1) || result["report"] != "captured answer" || stream["status"] != "error" || stream["stream_closed"] != true {
				t.Fatalf("interrupted outcome did not survive repeated reads/restart: result=%v stream=%v", result, stream)
			}
			again, err := os.ReadFile(path)
			if err != nil || string(again) != string(data) {
				t.Fatalf("terminal reconciliation changed the saved outcome again: %v", err)
			}
		})
	}
}

func TestCaptureWriteFailureClosesWithExplicitPartialOutput(t *testing.T) {
	oldStore, oldSchedules := globalStore, globalSchedules
	base := t.TempDir()
	store, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	globalStore, globalSchedules = store, nil
	t.Cleanup(func() { globalStore, globalSchedules = oldStore, oldSchedules })
	wrapper := filepath.Join(base, "rpc-child")
	script := "#!/bin/sh\nprintf '{\"type\":\"ready\"}\\n'\nwhile IFS= read -r line; do printf '{\"type\":\"agent_start\"}\\n'; done\n"
	if err := os.WriteFile(wrapper, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OMP_BIN", wrapper)
	t.Setenv("HOME", base)
	res, receipt := callTool(t, handleRun, map[string]any{"prompt": "work", "model": "provider/model", "cwd": base, "steerable": true})
	if res.IsError {
		t.Fatalf("launch failed: %v", receipt)
	}
	id, _ := receipt["job_id"].(string)
	job := store.get(id)
	if job == nil {
		t.Fatal("launched job missing")
	}
	t.Cleanup(func() {
		if job.cancelFn != nil {
			job.cancelFn()
		}
		select {
		case <-job.monitorDone:
		case <-time.After(10 * time.Second):
			t.Error("capture-failed child did not finish")
		}
	})
	job.mu.RLock()
	child := job.rpc
	job.mu.RUnlock()
	select {
	case <-child.ready:
	case <-time.After(10 * time.Second):
		t.Fatal("child did not become ready")
	}

	if err := child.logFile.Close(); err != nil {
		t.Fatal(err)
	}
	child.reply(map[string]any{"type": "get_state"})
	select {
	case <-job.monitorDone:
	case <-time.After(10 * time.Second):
		t.Fatal("capture failure left the process monitor running")
	}
	res, page := callTool(t, handleEvents, map[string]any{"job_id": id})
	if res.IsError || page["stream_closed"] != true || page["has_more"] != false || page["status"] != "error" {
		t.Fatalf("failed capture remained open or claimed success: %v", page)
	}
	if diagnostic, ok := page["capture_error"].(string); !ok || diagnostic == "" {
		t.Fatalf("incomplete capture was presented without a diagnostic: %v", page)
	}
	globalStore, err = newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	_, result := callTool(t, handleResult, map[string]any{"job_id": id})
	if diagnostic, ok := result["capture_error"].(string); !ok || diagnostic == "" ||
		result["status"] != "error" || result["exit_code"] != float64(-1) {
		t.Fatalf("restart lost the interrupted capture outcome: %v", result)
	}
}

func TestRPCResultWithoutAssistantOutputDoesNotExposeControlLog(t *testing.T) {
	oldStore := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = oldStore })
	path := filepath.Join(store.logsDir, "sub_no_report.log")
	if err := os.WriteFile(path, []byte("{\"type\":\"response\",\"command\":\"get_state\",\"data\":{\"systemPrompt\":\"PRIVATE_CONTROL_CONTEXT\"}}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	store.add(&Job{ID: "sub_no_report", Machine: "remote-test", Steerable: true, Status: "canceled", LogPath: path})
	res, result := callTool(t, handleResult, map[string]any{"job_id": "sub_no_report"})
	if res.IsError || result["report"] != "" {
		t.Fatalf("RPC control data was promoted to an assistant report: %v", result)
	}
}

func TestRestoredJobReplaysBoundedTailOnlyOnFirstRead(t *testing.T) {
	oldStore := globalStore
	oldTail := rpcRecoveryTailBytes
	base := t.TempDir()
	store, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	rpcRecoveryTailBytes = 512
	t.Cleanup(func() { globalStore, rpcRecoveryTailBytes = oldStore, oldTail })
	path := filepath.Join(store.logsDir, "sub_restored.log")
	stale := "{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"stale answer\"}]}}\n"
	filler := strings.Repeat("{\"type\":\"tool_execution_start\",\"toolName\":\"read\"}\n", 40)
	recent := "{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"final answer\"}]}}\n"
	if err := os.WriteFile(path, []byte(stale+filler+recent), 0600); err != nil {
		t.Fatal(err)
	}
	record := map[string]any{
		"id": "sub_restored", "status": "done", "steerable": true, "log_path": path,
		"model_requested": "provider/requested:high", "metadata_version": 0,
	}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store.dataDir, "sub_restored.json"), data, 0600); err != nil {
		t.Fatal(err)
	}

	loaded, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	globalStore = loaded
	job := loaded.get("sub_restored")
	if job == nil || job.Report != "" {
		t.Fatalf("startup replayed the RPC log: %+v", job)
	}
	_, result := callTool(t, handleResult, map[string]any{"job_id": "sub_restored"})
	if result["report"] != "final answer" {
		t.Fatalf("first read did not recover the newest captured output: %v", result)
	}
	persisted, err := os.ReadFile(filepath.Join(loaded.dataDir, "sub_restored.json"))
	if err != nil {
		t.Fatal(err)
	}
	var saved Job
	if err := json.Unmarshal(persisted, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Report != "final answer" || saved.MetadataVersion != metadataVersion {
		t.Fatalf("recovery was not persisted for later restarts: report=%q version=%d", saved.Report, saved.MetadataVersion)
	}
	reloaded, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	globalStore = reloaded
	again := reloaded.get("sub_restored")
	if again == nil || again.needsMetadataReplay || reportOf(again) != "final answer" {
		t.Fatalf("a recovered job replayed again after restart: replay=%v report=%q", again != nil && again.needsMetadataReplay, reportOf(again))
	}
}

func reportOf(j *Job) string {
	if j == nil {
		return ""
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.Report
}
