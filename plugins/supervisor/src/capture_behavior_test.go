package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func progressFrame(callID string, durationMs int) string {
	return fmt.Sprintf(`{"type":"tool_execution_update","toolCallId":%q,"toolName":"task","partialResult":{"details":{"progress":[{"id":"Worker","status":"running","durationMs":%d}]}}}`+"\n", callID, durationMs)
}

func runReadLoop(t *testing.T, input string) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), "child.log")
	logFile, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	job := &Job{ID: "sub_capture", Status: "running"}
	child := &rpcChild{job: job, logFile: logFile, terminal: make(chan struct{}, 1)}
	child.readLoop(io.NopCloser(strings.NewReader(input)))
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestCapturePersistsOnlyNewestProgressBeforeToolEnd(t *testing.T) {
	t.Setenv("SUPERVISOR_CAPTURE_COALESCE_MS", "60000")
	start := `{"type":"tool_execution_start","toolCallId":"call_a","toolName":"task","args":{}}` + "\n"
	end := `{"type":"tool_execution_end","toolCallId":"call_a","toolName":"task","result":{}}` + "\n"
	message := `{"type":"message_update","message":{"role":"assistant"}}` + "\n"
	var input strings.Builder
	input.WriteString(start)
	for i := 1; i <= 50; i++ {
		input.WriteString(progressFrame("call_a", i*150))
		if i == 25 {
			input.WriteString(message)
		}
	}
	input.WriteString(end)
	want := start + progressFrame("call_a", 150) + message + progressFrame("call_a", 50*150) + end
	if got := runReadLoop(t, input.String()); string(got) != want {
		t.Fatalf("capture kept superseded progress or lost ordering:\n got %d bytes\nwant %d bytes\n%s", len(got), len(want), got)
	}
}

func TestCaptureFlushesHeldProgressAtEOFWithoutAddingNewline(t *testing.T) {
	t.Setenv("SUPERVISOR_CAPTURE_COALESCE_MS", "60000")
	start := `{"type":"tool_execution_start","toolCallId":"call_b","toolName":"task","args":{}}` + "\n"
	partial := `{"type":"agent_end","messages":[]}`
	input := start + progressFrame("call_b", 1) + progressFrame("call_b", 2) + progressFrame("call_b", 3) + partial
	want := start + progressFrame("call_b", 1) + progressFrame("call_b", 3) + partial
	if got := runReadLoop(t, input); string(got) != want {
		t.Fatalf("child exit lost a held frame or changed the tail:\n%s", got)
	}
}

func TestEventsReadCompressedCaptureWithSameCursors(t *testing.T) {
	oldStore := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = oldStore })
	body := "{\"type\":\"agent_start\"}\n" + progressFrame("call_c", 1) + "{\"type\":\"response\",\"command\":\"get_state\"}\n" +
		"{\"type\":\"agent_end\",\"messages\":[]}\n"
	logPath := filepath.Join(store.logsDir, "sub_gz.log")
	var compressed bytes.Buffer
	zw := gzip.NewWriter(&compressed)
	zw.Write([]byte(body))
	zw.Close()
	if err := os.WriteFile(logPath+".gz", compressed.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	store.add(&Job{ID: "sub_gz", Status: "done", Steerable: true, LogPath: logPath})
	var cursor float64
	var types []string
	for range 10 {
		res, page := callTool(t, handleEvents, map[string]any{"job_id": "sub_gz", "after": cursor, "limit": 1})
		if res.IsError {
			t.Fatalf("compressed capture unreadable: %v", page)
		}
		for _, event := range page["events"].([]any) {
			types = append(types, event.(map[string]any)["event"].(map[string]any)["type"].(string))
		}
		cursor = page["next_cursor"].(float64)
		if page["has_more"] != true {
			break
		}
	}
	if cursor != float64(len(body)) || strings.Join(types, ",") != "agent_start,tool_execution_update,agent_end" {
		t.Fatalf("compressed paging = cursor %v types %v", cursor, types)
	}
	page, err := readJobEvents(context.Background(), logPath, 5, 100)
	if err != nil || !page.Reset || len(page.Events) != 3 {
		t.Fatalf("mid-frame cursor into a compressed capture did not reset: %+v %v", page, err)
	}
}
