package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

type fakeRPCChild struct {
	mu       sync.Mutex
	commands []map[string]any
	reject   map[string]string
	state    rpcState
	toChild  io.Reader
	toParent io.WriteCloser
	stdinEOF chan struct{}
}

func newFakeRPCChild(toChild io.Reader, toParent io.WriteCloser) *fakeRPCChild {
	f := &fakeRPCChild{toChild: toChild, toParent: toParent, stdinEOF: make(chan struct{})}
	go f.loop()
	return f
}

func (f *fakeRPCChild) loop() {
	scanner := bufio.NewScanner(f.toChild)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var cmd map[string]any
		if err := json.Unmarshal([]byte(line), &cmd); err != nil {
			continue
		}
		kind, _ := cmd["type"].(string)
		f.mu.Lock()
		f.commands = append(f.commands, cmd)
		state := f.state
		rejectMsg := f.reject[kind]
		f.mu.Unlock()

		if kind == "extension_ui_response" {
			continue
		}
		resp := map[string]any{"id": cmd["id"], "type": "response", "command": kind, "success": true}
		if rejectMsg != "" {
			resp["success"] = false
			resp["error"] = rejectMsg
			f.emit(resp)
			continue
		}
		if kind == "get_state" {
			resp["data"] = map[string]any{
				"isStreaming":        state.IsStreaming,
				"queuedMessageCount": state.QueuedMessageCount,
				"sessionId":          state.SessionID,
			}
		}
		f.emit(resp)
	}
	close(f.stdinEOF)
}

func (f *fakeRPCChild) emit(frame map[string]any) {
	data, err := json.Marshal(frame)
	if err != nil {
		return
	}
	_, _ = f.toParent.Write(append(data, '\n'))
}

func (f *fakeRPCChild) setState(st rpcState) {
	f.mu.Lock()
	f.state = st
	f.mu.Unlock()
}

func (f *fakeRPCChild) received() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]any, len(f.commands))
	copy(out, f.commands)
	return out
}

func (f *fakeRPCChild) waitForCommand(t *testing.T, kind string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, cmd := range f.received() {
			if cmd["type"] == kind {
				return cmd
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("child never received a %q command; got %v", kind, f.received())
	return nil
}

func (f *fakeRPCChild) waitForNthCommand(t *testing.T, kind string, n int) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var matches []map[string]any
		for _, cmd := range f.received() {
			if cmd["type"] == kind {
				matches = append(matches, cmd)
			}
		}
		if len(matches) >= n {
			return matches[n-1]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("child never received %d %q command(s); got %v", n, kind, f.received())
	return nil
}

func newTestStores(t *testing.T) string {
	t.Helper()
	tempDir := t.TempDir()
	store, err := newJobStore(tempDir)
	if err != nil {
		t.Fatalf("newJobStore: %v", err)
	}
	globalStore = store
	schedules, err := newScheduleStore(tempDir)
	if err != nil {
		t.Fatalf("newScheduleStore: %v", err)
	}
	globalSchedules = schedules
	asks, err := newGrokAskStore(tempDir)
	if err != nil {
		t.Fatalf("newGrokAskStore: %v", err)
	}
	globalGrokAsks = asks
	return tempDir
}

func callTool(t *testing.T, handler func(context.Context, mcp.CallToolRequest) (*mcp.CallToolResult, error), args map[string]any) (*mcp.CallToolResult, map[string]any) {
	t.Helper()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = args
	res, err := handler(context.Background(), req)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	payload := map[string]any{}
	if len(res.Content) > 0 {
		if text, ok := res.Content[0].(mcp.TextContent); ok {
			_ = json.Unmarshal([]byte(text.Text), &payload)
			if res.IsError {
				payload["__error_text"] = text.Text
			}
		}
	}
	return res, payload
}

func TestSteerDeliversOverRPCAndFinishesTurn(t *testing.T) {
	tempDir := newTestStores(t)

	childStdin, supervisorWrites := io.Pipe()
	supervisorReads, childStdout := io.Pipe()
	fake := newFakeRPCChild(childStdin, childStdout)

	logFile, err := os.Create(filepath.Join(tempDir, "rpc.log"))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer logFile.Close()

	job := &Job{
		ID:        "sub_rpc_steer",
		PID:       os.Getpid(),
		Status:    "running",
		Steerable: true,
		StartTime: time.Now(),
		CWD:       tempDir,
	}
	child := startRPCChild(globalStore, job, supervisorWrites, supervisorReads, logFile, "initial prompt")
	job.rpc = child
	globalStore.add(job)

	fake.emit(map[string]any{"type": "ready", "protocolVersion": 1})
	fake.waitForCommand(t, "prompt")

	fake.setState(rpcState{IsStreaming: true})
	_, payload := callTool(t, handleSteer, map[string]any{
		"job_id": job.ID,
		"text":   "also update the changelog",
		"mode":   "follow_up",
	})
	if payload["delivered"] != true {
		t.Fatalf("unexpected steer payload: %v", payload)
	}
	if payload["streaming"] != true {
		t.Errorf("expected live streaming state to be reported: %v", payload)
	}

	fake.setState(rpcState{IsStreaming: false, QueuedMessageCount: 0})
	fake.emit(map[string]any{
		"type":       "agent_end",
		"isTerminal": true,
		"messages": []map[string]any{
			{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "final answer"}}},
		},
	})

	select {
	case <-fake.stdinEOF:
	case <-time.After(10 * time.Second):
		t.Fatalf("supervisor never closed rpc stdin after the terminal turn")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job.mu.RLock()
		status, report := job.Status, job.Report
		job.mu.RUnlock()
		if status == "done" && report == "final answer" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	job.mu.RLock()
	defer job.mu.RUnlock()
	t.Fatalf("expected job done with the final assistant text, got status=%s report=%q", job.Status, job.Report)
}

func TestSteerRejectsUnsteerableJobs(t *testing.T) {
	newTestStores(t)

	printJob := &Job{ID: "sub_print", PID: os.Getpid(), Status: "running", StartTime: time.Now()}
	globalStore.add(printJob)
	doneJob := &Job{ID: "sub_done", Status: "done", StartTime: time.Now(), EndTime: time.Now()}
	globalStore.add(doneJob)

	res, payload := callTool(t, handleSteer, map[string]any{"job_id": "sub_print", "text": "hi"})
	if !res.IsError {
		t.Fatalf("expected an error steering a print-mode job")
	}
	msg, _ := payload["__error_text"].(string)
	if !strings.Contains(msg, "print mode") || !strings.Contains(msg, "steerable") {
		t.Errorf("print-mode error must explain how to run steerable jobs, got: %s", msg)
	}

	res, payload = callTool(t, handleSteer, map[string]any{"job_id": "sub_done", "text": "hi"})
	if !res.IsError {
		t.Fatalf("a finished job without a session cannot continue: %v", payload)
	}
	msg, _ = payload["__error_text"].(string)
	if !strings.Contains(msg, "no resumable OMP session") {
		t.Errorf("sessionless terminal-job error must explain why continuation is impossible, got: %s", msg)
	}

	res, _ = callTool(t, handleSteer, map[string]any{"job_id": "sub_missing", "text": "hi"})
	if !res.IsError {
		t.Fatalf("expected an error for an unknown job")
	}

	res, _ = callTool(t, handleSteer, map[string]any{"job_id": "sub_print"})
	if !res.IsError {
		t.Fatalf("expected an error when no text is given")
	}

	res, _ = callTool(t, handleSteer, map[string]any{"job_id": "sub_print", "text": "hi", "mode": "yell"})
	if !res.IsError {
		t.Fatalf("expected an error for an unsupported mode")
	}
}

func TestLastAssistantText(t *testing.T) {
	messages := []rpcMessage{
		{Role: "user", Content: json.RawMessage(`[{"type":"text","text":"question"}]`)},
		{Role: "assistant", Content: json.RawMessage(`[{"type":"text","text":"first"}]`)},

		{Role: "custom", Content: json.RawMessage(`"<system-notice>bg job done</system-notice>"`)},
		{Role: "assistant", Content: json.RawMessage(`[{"type":"thinking","text":"hidden"},{"type":"text","text":"second"}]`)},
	}
	if got := lastAssistantText(messages); got != "second" {
		t.Errorf("expected the newest assistant text, got %q", got)
	}
	if got := lastAssistantText(nil); got != "" {
		t.Errorf("expected empty text for no messages, got %q", got)
	}
}

func TestTerminalFrameWithStringContentFinishesJob(t *testing.T) {
	tempDir := newTestStores(t)

	childStdin, supervisorWrites := io.Pipe()
	supervisorReads, childStdout := io.Pipe()
	fake := newFakeRPCChild(childStdin, childStdout)

	logFile, err := os.Create(filepath.Join(tempDir, "rpc-mixed.log"))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer logFile.Close()

	job := &Job{ID: "sub_rpc_mixed", PID: os.Getpid(), Status: "running", Steerable: true, StartTime: time.Now(), CWD: tempDir}
	job.rpc = startRPCChild(globalStore, job, supervisorWrites, supervisorReads, logFile, "do work")
	globalStore.add(job)

	fake.emit(map[string]any{"type": "ready", "protocolVersion": 1})
	fake.waitForCommand(t, "prompt")

	fake.emit(map[string]any{
		"type":       "agent_end",
		"isTerminal": true,
		"messages": []map[string]any{
			{"role": "custom", "customType": "async-result", "content": "<system-notice>Background job bg_1 completed</system-notice>"},
			{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "STEER-DONE"}}},
		},
	})

	select {
	case <-fake.stdinEOF:
	case <-time.After(10 * time.Second):
		t.Fatalf("mixed-content terminal frame did not finish the job")
	}
	job.mu.RLock()
	defer job.mu.RUnlock()
	if job.Status != "done" || job.Report != "STEER-DONE" {
		t.Errorf("expected done/STEER-DONE, got %s/%q", job.Status, job.Report)
	}
}

func TestForceFinishWhenStreamingStuckAfterTerminal(t *testing.T) {
	prev := rpcSettleTimeout
	rpcSettleTimeout = 2 * time.Second
	t.Cleanup(func() { rpcSettleTimeout = prev })

	tempDir := newTestStores(t)

	childStdin, supervisorWrites := io.Pipe()
	supervisorReads, childStdout := io.Pipe()
	fake := newFakeRPCChild(childStdin, childStdout)

	logFile, err := os.Create(filepath.Join(tempDir, "rpc-stuck.log"))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer logFile.Close()

	job := &Job{ID: "sub_rpc_stuck", PID: os.Getpid(), Status: "running", Steerable: true, StartTime: time.Now(), CWD: tempDir}
	job.rpc = startRPCChild(globalStore, job, supervisorWrites, supervisorReads, logFile, "do work")
	globalStore.add(job)

	fake.emit(map[string]any{"type": "ready", "protocolVersion": 1})
	fake.waitForCommand(t, "prompt")

	fake.setState(rpcState{IsStreaming: true, QueuedMessageCount: 0})
	fake.emit(map[string]any{
		"type":       "agent_end",
		"isTerminal": true,
		"messages": []map[string]any{
			{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "done but still streaming"}}},
		},
	})

	select {
	case <-fake.stdinEOF:
	case <-time.After(8 * time.Second):
		t.Fatalf("supervisor never force-closed rpc stdin after settle timeout with stuck streaming")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job.mu.RLock()
		status, report := job.Status, job.Report
		job.mu.RUnlock()
		if status == "done" && report == "done but still streaming" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	job.mu.RLock()
	defer job.mu.RUnlock()
	t.Fatalf("expected force-finished done job, got status=%s report=%q", job.Status, job.Report)
}

func TestSteerDuringSettleExtendsWait(t *testing.T) {
	prev := rpcSettleTimeout
	rpcSettleTimeout = 10 * time.Second
	t.Cleanup(func() { rpcSettleTimeout = prev })

	tempDir := newTestStores(t)

	childStdin, supervisorWrites := io.Pipe()
	supervisorReads, childStdout := io.Pipe()
	fake := newFakeRPCChild(childStdin, childStdout)

	logFile, err := os.Create(filepath.Join(tempDir, "rpc-extend.log"))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer logFile.Close()

	job := &Job{ID: "sub_rpc_extend", PID: os.Getpid(), Status: "running", Steerable: true, StartTime: time.Now(), CWD: tempDir}
	job.rpc = startRPCChild(globalStore, job, supervisorWrites, supervisorReads, logFile, "initial")
	globalStore.add(job)

	fake.emit(map[string]any{"type": "ready", "protocolVersion": 1})
	fake.waitForCommand(t, "prompt")

	fake.setState(rpcState{IsStreaming: true})
	fake.emit(map[string]any{
		"type":       "agent_end",
		"isTerminal": true,
		"messages": []map[string]any{
			{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "first turn"}}},
		},
	})

	fake.waitForNthCommand(t, "get_state", 2)
	_, payload := callTool(t, handleSteer, map[string]any{
		"job_id": job.ID,
		"text":   "keep going",
		"mode":   "follow_up",
	})
	if payload["delivered"] != true {
		t.Fatalf("expected steer to deliver during settle: %v", payload)
	}

	select {
	case <-fake.stdinEOF:
		t.Fatalf("stdin closed during settle after a mid-settle steer; settle should have aborted")
	case <-time.After(1500 * time.Millisecond):
	}

	fake.setState(rpcState{IsStreaming: false, QueuedMessageCount: 0})
	fake.emit(map[string]any{
		"type":       "agent_end",
		"isTerminal": true,
		"messages": []map[string]any{
			{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "after steer"}}},
		},
	})

	select {
	case <-fake.stdinEOF:
	case <-time.After(10 * time.Second):
		t.Fatalf("supervisor never closed stdin after the post-steer terminal turn")
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		job.mu.RLock()
		status, report := job.Status, job.Report
		job.mu.RUnlock()
		if status == "done" && report == "after steer" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	job.mu.RLock()
	defer job.mu.RUnlock()
	t.Fatalf("expected done/after steer, got status=%s report=%q", job.Status, job.Report)
}

func withShortRPCTimers(t *testing.T, settle, idle, grace time.Duration) {
	t.Helper()
	prevSettle, prevIdle, prevGrace := rpcSettleTimeout, rpcIdleTimeout, rpcSendGrace
	rpcSettleTimeout, rpcIdleTimeout, rpcSendGrace = settle, idle, grace
	t.Cleanup(func() {
		rpcSettleTimeout, rpcIdleTimeout, rpcSendGrace = prevSettle, prevIdle, prevGrace
	})
}

func waitForJobDone(t *testing.T, job *Job, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		job.mu.RLock()
		status := job.Status
		job.mu.RUnlock()
		if status == "done" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	job.mu.RLock()
	defer job.mu.RUnlock()
	t.Fatalf("job never reached done; status=%s", job.Status)
}

func TestSteerDuringSettleForceFinishesWhenTurnNeverEnds(t *testing.T) {

	withShortRPCTimers(t, 10*time.Second, 2*time.Second, 200*time.Millisecond)

	tempDir := newTestStores(t)

	childStdin, supervisorWrites := io.Pipe()
	supervisorReads, childStdout := io.Pipe()
	fake := newFakeRPCChild(childStdin, childStdout)

	logFile, err := os.Create(filepath.Join(tempDir, "rpc-stall.log"))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer logFile.Close()

	job := &Job{ID: "sub_rpc_stall", PID: os.Getpid(), Status: "running", Steerable: true, StartTime: time.Now(), CWD: tempDir}
	job.rpc = startRPCChild(globalStore, job, supervisorWrites, supervisorReads, logFile, "initial")
	globalStore.add(job)

	fake.emit(map[string]any{"type": "ready", "protocolVersion": 1})
	fake.waitForCommand(t, "prompt")

	fake.setState(rpcState{IsStreaming: true})
	fake.emit(map[string]any{
		"type":       "agent_end",
		"isTerminal": true,
		"messages": []map[string]any{
			{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "first turn"}}},
		},
	})

	fake.waitForNthCommand(t, "get_state", 2)
	_, payload := callTool(t, handleSteer, map[string]any{
		"job_id": job.ID,
		"text":   "keep going",
		"mode":   "follow_up",
	})
	if payload["delivered"] != true {
		t.Fatalf("expected steer to deliver during settle: %v", payload)
	}

	start := time.Now()
	select {
	case <-fake.stdinEOF:
	case <-time.After(8 * time.Second):
		t.Fatalf("supervisor never closed rpc stdin; a steer during settle stranded the job")
	}
	if elapsed := time.Since(start); elapsed >= 8*time.Second {
		t.Fatalf("stdin closed too late (%s); the idle watchdog did not fire", elapsed)
	}
	waitForJobDone(t, job, 3*time.Second)
}

func TestSteeredTurnWithoutTerminalFinishesWhenSessionGoesIdle(t *testing.T) {

	withShortRPCTimers(t, 10*time.Second, 60*time.Second, 200*time.Millisecond)

	tempDir := newTestStores(t)

	childStdin, supervisorWrites := io.Pipe()
	supervisorReads, childStdout := io.Pipe()
	fake := newFakeRPCChild(childStdin, childStdout)

	logFile, err := os.Create(filepath.Join(tempDir, "rpc-quiet.log"))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer logFile.Close()

	job := &Job{ID: "sub_rpc_quiet", PID: os.Getpid(), Status: "running", Steerable: true, StartTime: time.Now(), CWD: tempDir}
	job.rpc = startRPCChild(globalStore, job, supervisorWrites, supervisorReads, logFile, "initial")
	globalStore.add(job)

	fake.emit(map[string]any{"type": "ready", "protocolVersion": 1})
	fake.waitForCommand(t, "prompt")

	fake.setState(rpcState{IsStreaming: true})
	fake.emit(map[string]any{
		"type":       "agent_end",
		"isTerminal": true,
		"messages": []map[string]any{
			{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "first turn"}}},
		},
	})

	fake.waitForNthCommand(t, "get_state", 2)
	_, payload := callTool(t, handleSteer, map[string]any{
		"job_id": job.ID,
		"text":   "one more thing",
		"mode":   "steer",
	})
	if payload["delivered"] != true {
		t.Fatalf("expected steer to deliver during settle: %v", payload)
	}

	fake.setState(rpcState{IsStreaming: false, QueuedMessageCount: 0})

	select {
	case <-fake.stdinEOF:
	case <-time.After(15 * time.Second):
		t.Fatalf("supervisor never closed rpc stdin for an idle session with no terminal agent_end")
	}
	waitForJobDone(t, job, 3*time.Second)
}

func TestSetWidgetGetsUIResponse(t *testing.T) {
	tempDir := newTestStores(t)

	childStdin, supervisorWrites := io.Pipe()
	supervisorReads, childStdout := io.Pipe()
	fake := newFakeRPCChild(childStdin, childStdout)

	logFile, err := os.Create(filepath.Join(tempDir, "rpc-widget.log"))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer logFile.Close()

	job := &Job{ID: "sub_rpc_widget", PID: os.Getpid(), Status: "running", Steerable: true, StartTime: time.Now(), CWD: tempDir}
	job.rpc = startRPCChild(globalStore, job, supervisorWrites, supervisorReads, logFile, "widget prompt")
	globalStore.add(job)

	fake.emit(map[string]any{"type": "ready", "protocolVersion": 1})
	fake.waitForCommand(t, "prompt")

	fake.emit(map[string]any{
		"type":      "extension_ui_request",
		"id":        "ui-widget-1",
		"method":    "setWidget",
		"widgetKey": "autoresearch",
	})

	resp := fake.waitForCommand(t, "extension_ui_response")
	if resp["id"] != "ui-widget-1" {
		t.Errorf("expected response for ui-widget-1, got %v", resp)
	}
	if resp["success"] != true {
		t.Errorf("expected success ack for setWidget, got %v", resp)
	}

	fake.emit(map[string]any{
		"type":   "extension_ui_request",
		"id":     "ui-confirm-1",
		"method": "confirm",
	})
	resp2 := fake.waitForNthCommand(t, "extension_ui_response", 2)
	if resp2["id"] != "ui-confirm-1" {
		t.Errorf("expected response for ui-confirm-1, got %v", resp2)
	}
	if resp2["cancelled"] != true {
		t.Errorf("expected cancelled for confirm, got %v", resp2)
	}

	fake.setState(rpcState{})
	fake.emit(map[string]any{"type": "agent_end", "isTerminal": true, "messages": []map[string]any{}})
	select {
	case <-fake.stdinEOF:
	case <-time.After(10 * time.Second):
		t.Fatalf("job did not finish after widget acks")
	}
}

func TestElidedTerminalTranscriptFinishesJobWithTurnReport(t *testing.T) {
	tempDir := newTestStores(t)

	childStdin, supervisorWrites := io.Pipe()
	supervisorReads, childStdout := io.Pipe()
	fake := newFakeRPCChild(childStdin, childStdout)

	logFile, err := os.Create(filepath.Join(tempDir, "rpc-elided.log"))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer logFile.Close()

	job := &Job{ID: "sub_rpc_elided", PID: os.Getpid(), Status: "running", Steerable: true, StartTime: time.Now(), CWD: tempDir}
	job.rpc = startRPCChild(globalStore, job, supervisorWrites, supervisorReads, logFile, "write the report")
	globalStore.add(job)

	fake.emit(map[string]any{"type": "ready", "protocolVersion": 1})
	fake.waitForCommand(t, "prompt")

	fake.emit(map[string]any{
		"type": "turn_end",
		"message": map[string]any{
			"role": "assistant",
			"content": []map[string]any{
				{"type": "thinking", "thinking": "compose the report"},
				{"type": "text", "text": "FINAL REPORT"},
			},
		},
	})

	fake.emit(map[string]any{
		"type":         "agent_end",
		"isTerminal":   true,
		"messageCount": 149,
		"messages": []any{
			map[string]any{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "early turn text"}}},
			map[string]any{"role": "toolResult", "toolName": "read", "content": []map[string]any{{"type": "text", "text": "file"}}},
			"…[85 items elided for RPC frame]",
		},
	})

	select {
	case <-fake.stdinEOF:
	case <-time.After(10 * time.Second):
		t.Fatalf("elided terminal frame never finished the job")
	}
	waitForJobDone(t, job, 3*time.Second)

	job.mu.RLock()
	defer job.mu.RUnlock()
	if job.Report != "FINAL REPORT" {
		t.Errorf("expected the turn's own report, got %q", job.Report)
	}
}

func TestDecodeFrameFallsBackToEnvelope(t *testing.T) {
	frame, err := decodeFrame([]byte(`{"type":"agent_end","isTerminal":true,"messages":"…[149 items elided for RPC frame]"}`))
	if err != nil {
		t.Fatalf("decodeFrame: %v", err)
	}
	if frame.Type != "agent_end" {
		t.Errorf("expected type agent_end, got %q", frame.Type)
	}
	if frame.IsTerminal == nil || !*frame.IsTerminal {
		t.Errorf("expected isTerminal to survive the fallback decode, got %v", frame.IsTerminal)
	}
	if _, err := decodeFrame([]byte(`{"type":`)); err == nil {
		t.Errorf("expected malformed JSON to stay an error")
	}
}

func TestSilentSessionWithoutTerminalFrameForceFinishes(t *testing.T) {

	withShortRPCTimers(t, 60*time.Second, 1*time.Second, 200*time.Millisecond)

	tempDir := newTestStores(t)

	childStdin, supervisorWrites := io.Pipe()
	supervisorReads, childStdout := io.Pipe()
	fake := newFakeRPCChild(childStdin, childStdout)

	logFile, err := os.Create(filepath.Join(tempDir, "rpc-noterminal.log"))
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer logFile.Close()

	job := &Job{ID: "sub_rpc_noterminal", PID: os.Getpid(), Status: "running", Steerable: true, StartTime: time.Now(), CWD: tempDir}
	job.rpc = startRPCChild(globalStore, job, supervisorWrites, supervisorReads, logFile, "do work")
	globalStore.add(job)

	fake.emit(map[string]any{"type": "ready", "protocolVersion": 1})
	fake.waitForCommand(t, "prompt")

	fake.setState(rpcState{SessionID: "01a074f9-9990-7000-8000-000000000012"})

	select {
	case <-fake.stdinEOF:
	case <-time.After(20 * time.Second):
		t.Fatalf("supervisor never closed rpc stdin; a missing terminal frame stranded the job")
	}
	waitForJobDone(t, job, 3*time.Second)

	job.mu.RLock()
	defer job.mu.RUnlock()
	if job.SessionID != "01a074f9-9990-7000-8000-000000000012" {
		t.Errorf("expected the session id reported by get_state, got %q", job.SessionID)
	}
}

func TestRejectedFollowupDoesNotRewritePriorOutcome(t *testing.T) {
	job := &Job{Status: "running", Report: "completed task output"}
	child := &rpcChild{job: job, initialPromptID: "initial", terminal: make(chan struct{}, 1)}
	rejected := false
	child.handleFrame(rpcFrame{Type: "response", Command: "prompt", ID: "followup", Success: &rejected, Error: "follow-up rejected"})
	child.forceFinish()
	if job.Status != "done" || job.ExitCode != 0 || job.Report != "completed task output" {
		t.Fatalf("rejected follow-up corrupted the completed run: status=%s exit=%d report=%q", job.Status, job.ExitCode, job.Report)
	}
}

func TestSetEventFilterPrecedesInitialPrompt(t *testing.T) {
	tempDir := newTestStores(t)
	childStdin, supervisorWrites := io.Pipe()
	supervisorReads, childStdout := io.Pipe()
	t.Cleanup(func() {
		supervisorWrites.Close()
		childStdout.Close()
	})
	fake := newFakeRPCChild(childStdin, childStdout)
	logFile, err := os.Create(filepath.Join(tempDir, "filter.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logFile.Close() })
	job := &Job{
		ID: "sub_rpc_filter", PID: os.Getpid(), Status: "running", Steerable: true,
		StartTime: time.Now(), CWD: tempDir,
	}
	job.rpc = startRPCChild(globalStore, job, supervisorWrites, supervisorReads, logFile, "initial prompt")
	globalStore.add(job)
	fake.emit(map[string]any{"type": "ready", "protocolVersion": 1})
	fake.waitForCommand(t, "prompt")
	filterAt, promptAt := -1, -1
	for i, cmd := range fake.received() {
		switch cmd["type"] {
		case "set_event_filter":
			if filterAt >= 0 {
				continue
			}
			filterAt = i
			if _, ok := cmd["events"]; !ok || cmd["events"] != nil {
				t.Fatalf("events filter = %#v", cmd["events"])
			}
			if cmd["messageUpdates"] != "delta" {
				t.Fatalf("messageUpdates = %#v", cmd["messageUpdates"])
			}
		case "prompt":
			if promptAt < 0 {
				promptAt = i
			}
		}
	}
	if filterAt < 0 || promptAt < 0 || filterAt > promptAt {
		t.Fatalf("filter index %d prompt index %d", filterAt, promptAt)
	}
}

func TestSetEventFilterRejectionKeepsFullMode(t *testing.T) {
	tempDir := newTestStores(t)
	childStdin, supervisorWrites := io.Pipe()
	supervisorReads, childStdout := io.Pipe()
	t.Cleanup(func() {
		supervisorWrites.Close()
		childStdout.Close()
	})
	fake := newFakeRPCChild(childStdin, childStdout)
	fake.mu.Lock()
	fake.reject = map[string]string{"set_event_filter": "unknown command"}
	fake.mu.Unlock()
	logFile, err := os.Create(filepath.Join(tempDir, "filter-legacy.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { logFile.Close() })
	job := &Job{
		ID: "sub_rpc_filter_legacy", PID: os.Getpid(), Status: "running", Steerable: true,
		StartTime: time.Now(), CWD: tempDir,
	}
	startRPCChild(globalStore, job, supervisorWrites, supervisorReads, logFile, "initial prompt")
	globalStore.add(job)
	fake.emit(map[string]any{"type": "ready", "protocolVersion": 1})
	prompt := fake.waitForCommand(t, "prompt")
	if prompt["message"] != "initial prompt" {
		t.Fatalf("prompt = %#v", prompt)
	}
	job.mu.RLock()
	status := job.Status
	job.mu.RUnlock()
	if status != "running" {
		t.Fatalf("rejected filter changed status to %s", status)
	}
}
