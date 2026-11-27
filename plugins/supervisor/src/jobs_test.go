package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func retainedTestStore(t *testing.T) *JobStore {
	t.Helper()
	previous := globalStore
	store := &JobStore{jobs: make(map[string]*Job)}
	globalStore = store
	t.Cleanup(func() { globalStore = previous })
	return store
}

func retainedJobIDs(t *testing.T, payload map[string]any) []string {
	t.Helper()
	rows, ok := payload["jobs"].([]any)
	if !ok {
		t.Fatalf("jobs must be an array: %v", payload)
	}
	ids := make([]string, 0, len(rows))
	for _, raw := range rows {
		row, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("invalid retained job: %v", raw)
		}
		id, ok := row["job_id"].(string)
		if !ok {
			t.Fatalf("job has no identity: %v", row)
		}
		ids = append(ids, id)
	}
	return ids
}

func TestJobsRetainsAllStatusesWithStableNewestFirstPages(t *testing.T) {
	store := retainedTestStore(t)
	t.Setenv("SUPERVISOR_MACHINE", "local-test")
	start := time.Date(2026, 9, 25, 0, 0, 0, 0, time.UTC)
	for _, job := range []*Job{
		{ID: "sub_old_done", Status: "done", StartTime: start},
		{ID: "sub_b_error", Status: "error", StartTime: start.Add(time.Hour), Steerable: true},
		{ID: "sub_new_canceled", Status: "canceled", StartTime: start.Add(2 * time.Hour)},
		{ID: "sub_a_running", Status: "running", StartTime: start.Add(time.Hour),
			Machine: "remote-test", ModelResolved: "provider/observed", ParentSessionID: "foreign-parent", RootSessionID: "foreign-root"},
	} {
		store.jobs[job.ID] = job
	}
	res, first := callTool(t, handleJobs, map[string]any{"limit": 2})
	if res.IsError || !reflect.DeepEqual(retainedJobIDs(t, first), []string{"sub_new_canceled", "sub_a_running"}) ||
		first["count"] != float64(2) || first["total"] != float64(4) || first["offset"] != float64(0) || first["next_offset"] != float64(2) {
		t.Fatalf("first page lost retained jobs or order: %v", first)
	}
	rows := first["jobs"].([]any)
	if rows[0].(map[string]any)["status"] != "canceled" || rows[0].(map[string]any)["machine"] != "local-test" {
		t.Fatalf("terminal metadata changed: %v", rows[0])
	}
	remote := rows[1].(map[string]any)
	if remote["status"] != "running" || remote["machine"] != "remote-test" || remote["model"] != "provider/observed" ||
		remote["parent_session_id"] != "foreign-parent" || remote["root_session_id"] != "foreign-root" {
		t.Fatalf("foreign linkage/observed metadata lost: %v", remote)
	}
	res, second := callTool(t, handleJobs, map[string]any{"limit": 2, "offset": first["next_offset"]})
	if res.IsError || !reflect.DeepEqual(retainedJobIDs(t, second), []string{"sub_b_error", "sub_old_done"}) ||
		second["count"] != float64(2) || second["total"] != float64(4) || second["offset"] != float64(2) || second["next_offset"] != nil {
		t.Fatalf("terminal page skipped or repeated jobs: %v", second)
	}
	rows = second["jobs"].([]any)
	if rows[0].(map[string]any)["status"] != "error" || rows[1].(map[string]any)["status"] != "done" {
		t.Fatalf("terminal statuses lost: %v", rows)
	}
	for _, offset := range []any{4, 5, json.Number("9007199254740991")} {
		res, page := callTool(t, handleJobs, map[string]any{"offset": offset})
		if res.IsError || len(retainedJobIDs(t, page)) != 0 || page["count"] != float64(0) || page["total"] != float64(4) || page["next_offset"] != nil {
			t.Fatalf("past-end offset %v must return an empty page: %v", offset, page)
		}
	}

	_, again := callTool(t, handleJobs, map[string]any{"limit": 2})
	if !reflect.DeepEqual(first, again) {
		t.Fatalf("same retained store produced a different first page: %v", again)
	}
}

func TestJobsListingDoesNotExposePrivateJobContent(t *testing.T) {
	store := retainedTestStore(t)
	const privateContent = "PRIVATE_JOB_CONTENT_REQUIRES_EXPLICIT_INSPECTION"
	store.jobs["sub_private"] = &Job{
		ID: "sub_private", Status: "done", StartTime: time.Unix(1, 0),
		Prompt: privateContent, Report: privateContent, CaptureError: privateContent,
		Title: privateContent, CWD: privateContent, LogPath: privateContent,
		FilesChanged: []string{privateContent},
	}
	res, payload := callTool(t, handleJobs, nil)
	if res.IsError || !reflect.DeepEqual(retainedJobIDs(t, payload), []string{"sub_private"}) {
		t.Fatalf("private job must remain discoverable by summary identity: %v", payload)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(privateContent)) {
		t.Fatal("global discovery exposed private job content before explicit inspection")
	}
	row := payload["jobs"].([]any)[0].(map[string]any)
	for _, field := range []string{"prompt", "report", "capture_error", "cwd", "log_path", "files_changed"} {
		if _, exposed := row[field]; exposed {
			t.Errorf("global discovery exposed private field %q", field)
		}
	}
}

func TestJobsBoundsPagesWithoutLosingRetainedTail(t *testing.T) {
	store := retainedTestStore(t)
	res, empty := callTool(t, handleJobs, nil)
	if res.IsError || !reflect.DeepEqual(retainedJobIDs(t, empty), []string{}) || empty["total"] != float64(0) || empty["next_offset"] != nil {
		t.Fatalf("empty store must return an empty jobs array: %v", empty)
	}
	for i := range 205 {
		id := fmt.Sprintf("sub_%03d", i)
		store.jobs[id] = &Job{ID: id, Status: "done", StartTime: time.Unix(int64(i), 0)}
	}
	for _, tc := range []struct {
		name  string
		args  map[string]any
		count int
		last  string
	}{
		{"default", nil, 50, "sub_155"},
		{"maximum", map[string]any{"limit": 1000}, 200, "sub_005"},
		{"minimum", map[string]any{"limit": 0}, 1, "sub_204"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, page := callTool(t, handleJobs, tc.args)
			ids := retainedJobIDs(t, page)
			if res.IsError || len(ids) != tc.count || ids[0] != "sub_204" || ids[len(ids)-1] != tc.last ||
				page["count"] != float64(tc.count) || page["total"] != float64(205) || page["next_offset"] != float64(tc.count) {
				t.Fatalf("bounded page lost its continuation: %v", page)
			}
		})
	}
	res, final := callTool(t, handleJobs, map[string]any{"limit": 200, "offset": 200})
	if res.IsError || !reflect.DeepEqual(retainedJobIDs(t, final), []string{"sub_004", "sub_003", "sub_002", "sub_001", "sub_000"}) || final["next_offset"] != nil {
		t.Fatalf("capped first page hid retained tail: %v", final)
	}
}

func TestJobsRejectsInvalidPagination(t *testing.T) {
	retainedTestStore(t)
	for _, key := range []string{"limit", "offset"} {
		for _, invalid := range []any{-1, 0.5, "2", nil, true, math.NaN(), math.Inf(1), float64(9007199254740992), json.Number("invalid")} {
			t.Run(fmt.Sprintf("%s/%v", key, invalid), func(t *testing.T) {
				res, payload := callTool(t, handleJobs, map[string]any{key: invalid})
				if !res.IsError {
					t.Fatalf("invalid %s=%v accepted: %v", key, invalid, payload)
				}
			})
		}
	}
}

func TestJobsListingDoesNotRecoverReconcileOrPersist(t *testing.T) {
	previous := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = previous })
	logPath := filepath.Join(store.logsDir, "sub_unrecovered.log")
	frame := `{"type":"response","command":"get_state","success":true,"data":{"model":{"provider":"observed","id":"from-log"}}}` + "\n"
	if err := os.WriteFile(logPath, []byte(frame), 0600); err != nil {
		t.Fatal(err)
	}
	job := &Job{
		ID: "sub_unrecovered", Status: "running", PID: 0, Steerable: true,
		LogPath: logPath, ModelRequested: "requested/model", recovered: true, needsMetadataReplay: true,
	}
	store.add(job)
	path := filepath.Join(store.dataDir, job.ID+".json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	res, page := callTool(t, handleJobs, nil)
	row := page["jobs"].([]any)[0].(map[string]any)
	if res.IsError || row["status"] != "running" || row["model"] != nil {
		t.Fatalf("listing reconciled the process or recovered a log: %v", page)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || !job.needsMetadataReplay || job.MetadataVersion != 0 || job.Status != "running" || job.ModelResolved != "" {
		t.Fatalf("listing changed retained state: %+v", job)
	}

	snapshot := store.retainedJobs(50, 0)
	job.mu.Lock()
	job.Status = "error"
	job.mu.Unlock()
	if snapshot.Jobs[0].Status != "running" {
		t.Fatalf("returned snapshot changed with its job: %+v", snapshot)
	}
}
