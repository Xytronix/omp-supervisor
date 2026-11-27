package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEmptyCaptureStaysPlain(t *testing.T) {
	store := compressionStore(t)
	logPath := filepath.Join(store.logsDir, "sub_empty.log")
	if err := os.WriteFile(logPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	job := &Job{ID: "sub_empty", Status: "done", Steerable: true, LogPath: logPath, captureDone: closedChan()}
	store.add(job)
	before, err := readJobEvents(context.Background(), logPath, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := compressFinishedCapture(store, job); err != nil {
		t.Fatal(err)
	}
	compressFinishedCaptures(store)
	if info, err := os.Stat(logPath); err != nil || info.Size() != 0 {
		t.Fatalf("empty capture not kept as .log: %v", err)
	}
	if _, err := os.Stat(logPath + ".gz"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty capture compressed: %v", err)
	}
	after, err := readJobEvents(context.Background(), logPath, 0, 100)
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("events changed: %v\n%s\n%s", err, a, b)
	}
	if sum, size, compressed, err := hashLocalCapture(logPath); err != nil || compressed || size != 0 || sum == "" {
		t.Fatalf("hashLocalCapture = %s %d %v %v", sum, size, compressed, err)
	}
}

func plainGzip(t *testing.T, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func eventPages(t *testing.T, logPath string) []byte {
	t.Helper()
	var pages []jobEventPage
	var cursor int64
	for range 1000 {
		page, err := readJobEvents(context.Background(), logPath, cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, page)
		if !page.HasMore {
			break
		}
		cursor = page.NextCursor
	}
	out, _ := json.Marshal(pages)
	return out
}

func TestSweepRepacksPlainGzipCapture(t *testing.T) {
	old := captureGzipChunk
	captureGzipChunk = 64
	t.Cleanup(func() { captureGzipChunk = old })
	store := compressionStore(t)
	var body strings.Builder
	body.WriteString(`{"type":"agent_start"}` + "\n")
	for i := range 30 {
		body.WriteString(progressFrame(fmt.Sprintf("c%d", i), i))
	}
	body.WriteString(`{"type":"agent_end","messages":[]}` + "\n")
	raw := []byte(body.String())
	compressed := plainGzip(t, raw)
	done := &Job{ID: "sub_plain_gz", Status: "done", Steerable: true, captureDone: closedChan()}
	live := &Job{ID: "sub_plain_live", Status: "running", Steerable: true, captureDone: make(chan struct{})}
	for _, job := range []*Job{done, live} {
		job.LogPath = filepath.Join(store.logsDir, job.ID+".log")
		if err := os.WriteFile(job.LogPath+".gz", compressed, 0600); err != nil {
			t.Fatal(err)
		}
		store.add(job)
	}
	before := eventPages(t, done.LogPath)
	if err := compressFinishedCapture(store, live); err == nil || !strings.Contains(err.Error(), "still active") {
		t.Fatalf("repack of a running job not refused: %v", err)
	}
	compressFinishedCaptures(store)
	if got, err := os.ReadFile(live.LogPath + ".gz"); err != nil || !bytes.Equal(got, compressed) {
		t.Fatalf("running job's capture changed: %v", err)
	}
	capture, err := openCapture(done.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	g := capture.(*gzipCapture)
	members, size := len(g.members), g.Size()
	capture.Close()
	if members < 10 || size != int64(len(raw)) {
		t.Fatalf("capture not re-packed: members %d size %d", members, size)
	}
	if after := eventPages(t, done.LogPath); !bytes.Equal(before, after) {
		t.Fatalf("events changed after repack:\n%s\n%s", before, after)
	}
	packed, err := os.ReadFile(done.LogPath + ".gz")
	if err != nil {
		t.Fatal(err)
	}
	if got := gunzipBytes(t, packed); !bytes.Equal(got, raw) {
		t.Fatal("re-packed capture is not one gzip stream of the original")
	}
	if _, err := os.Stat(done.LogPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("repack created a plain log: %v", err)
	}
	entries, _ := os.ReadDir(store.logsDir)
	if len(entries) != 2 {
		t.Fatalf("logs dir = %v", entries)
	}
}

func TestSweepRemovesOnlyOrphanedCaptureTemps(t *testing.T) {
	store := compressionStore(t)
	dir := store.logsDir
	orphans := []string{".sub_a.log.gz.tmp-123", ".sub_b.log.gz.tmp-9"}
	kept := []string{"sub_a.log", "sub_a.log.gz.tmp-1", ".sub_a.log.tmp-1", ".notes.tmp-1", ".sub_a.log.gz", "sub_c.log.gz"}
	for _, name := range append(append([]string{}, orphans...), kept...) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, ".sub_d.log.gz.tmp-dir"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(dir, "sub_a.log"), filepath.Join(dir, ".sub_e.log.gz.tmp-link")); err != nil {
		t.Fatal(err)
	}
	kept = append(kept, ".sub_d.log.gz.tmp-dir", ".sub_e.log.gz.tmp-link")
	compressFinishedCaptures(store)
	for _, name := range orphans {
		if _, err := os.Lstat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("orphaned temp %s kept: %v", name, err)
		}
	}
	for _, name := range kept {
		if _, err := os.Lstat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s removed: %v", name, err)
		}
	}
}

func TestSweepRemovesLogLeftBesideVerifiedGzip(t *testing.T) {
	store := compressionStore(t)
	logPath := filepath.Join(store.logsDir, "sub_left.log")
	body := []byte(`{"type":"agent_start"}` + "\n" + progressFrame("a", 1))
	if err := os.WriteFile(logPath, body, 0600); err != nil {
		t.Fatal(err)
	}
	writeIndexedGzip(t, logPath+".gz", body)
	gzBefore, err := os.Stat(logPath + ".gz")
	if err != nil {
		t.Fatal(err)
	}
	store.add(&Job{ID: "sub_left", Status: "done", Steerable: true, LogPath: logPath, captureDone: closedChan()})
	compressFinishedCaptures(store)
	if _, err := os.Stat(logPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("raw log kept beside a verified .gz: %v", err)
	}
	gzAfter, err := os.Stat(logPath + ".gz")
	if err != nil || !os.SameFile(gzBefore, gzAfter) {
		t.Fatalf("verified .gz was rewritten: %v", err)
	}
	page, err := readJobEvents(context.Background(), logPath, 0, 100)
	if err != nil || len(page.Events) != 2 {
		t.Fatalf("events after removal = %+v %v", page, err)
	}
}
