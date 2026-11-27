package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	at time.Time
	fn func()
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) schedule(d time.Duration, fn func()) *time.Timer {
	c.mu.Lock()
	c.timers = append(c.timers, &fakeTimer{at: c.now.Add(d), fn: fn})
	c.mu.Unlock()
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	return timer
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	var due []func()
	kept := c.timers[:0]
	for _, timer := range c.timers {
		if timer.at.After(c.now) {
			kept = append(kept, timer)
		} else {
			due = append(due, timer.fn)
		}
	}
	c.timers = kept
	c.mu.Unlock()
	for _, fn := range due {
		fn()
	}
}

func newTestCaptureWriter(out io.Writer, window time.Duration) (*captureWriter, *fakeClock) {
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	w := newCaptureWriter(out, window)
	w.now = clock.Now
	w.schedule = clock.schedule
	return w, clock
}

func streamFrame(callID string, generation int) string {
	return fmt.Sprintf(`{"type":"tool_stream_update","toolCallId":%q,"toolName":"edit","update":{"generation":%d}}`+"\n", callID, generation)
}

func TestCaptureWriterWritesNewestProgressOncePerWindow(t *testing.T) {
	var out bytes.Buffer
	w, clock := newTestCaptureWriter(&out, time.Second)
	write := func(s string) {
		t.Helper()
		if n, err := w.Write([]byte(s)); err != nil || n != len(s) {
			t.Fatalf("write = %d, %v", n, err)
		}
	}
	write(progressFrame("a", 0))
	want := progressFrame("a", 0)
	for i := 1; i <= 5; i++ {
		clock.Advance(150 * time.Millisecond)
		write(progressFrame("a", i))
	}
	if out.String() != want {
		t.Fatalf("superseded progress written inside the window: %s", out.String())
	}

	clock.Advance(250 * time.Millisecond)
	want += progressFrame("a", 5)
	if out.String() != want {
		t.Fatalf("quiet tool lost its newest snapshot: %s", out.String())
	}

	clock.Advance(100 * time.Millisecond)
	write(progressFrame("a", 6) + streamFrame("a", 1) + streamFrame("a", 2))
	want += streamFrame("a", 1)
	if out.String() != want {
		t.Fatalf("stream and execution updates shared a slot: %s", out.String())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	want += progressFrame("a", 6) + streamFrame("a", 2)
	if out.String() != want {
		t.Fatalf("close did not flush held frames in hold order: %s", out.String())
	}
}

func TestCaptureWriterFlushOrdering(t *testing.T) {
	cases := []struct {
		name, frame string
		flushAll    bool
	}{
		{"tool end flushes its own call", `{"type":"tool_execution_end","toolCallId":"a","toolName":"task"}` + "\n", false},
		{"agent_end flushes all", `{"type":"agent_end","messages":[]}` + "\n", true},
		{"agent_pause flushes all", `{"type":"agent_pause"}` + "\n", true},
		{"turn_end flushes all", `{"type":"turn_end","message":{}}` + "\n", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			w, clock := newTestCaptureWriter(&out, time.Second)
			w.Write([]byte(progressFrame("a", 1) + progressFrame("b", 1)))
			clock.Advance(10 * time.Millisecond)
			w.Write([]byte(progressFrame("a", 2) + progressFrame("b", 2) + streamFrame("a", 1)))
			clock.Advance(10 * time.Millisecond)
			w.Write([]byte(streamFrame("a", 2) + tc.frame))
			want := progressFrame("a", 1) + progressFrame("b", 1) + streamFrame("a", 1)
			if tc.flushAll {
				want += progressFrame("a", 2) + progressFrame("b", 2) + streamFrame("a", 2) + tc.frame
			} else {
				want += progressFrame("a", 2) + streamFrame("a", 2) + tc.frame
			}
			if out.String() != want {
				t.Fatalf("flush order:\n got %s\nwant %s", out.String(), want)
			}
			w.Close()
			if !tc.flushAll {
				want += progressFrame("b", 2)
			}
			if out.String() != want {
				t.Fatalf("close lost a held frame:\n got %s\nwant %s", out.String(), want)
			}
		})
	}
}

func TestCaptureWriterKeepsFramesWholeAcrossSplitWrites(t *testing.T) {
	var out bytes.Buffer
	w, _ := newTestCaptureWriter(&out, time.Second)
	input := `{"type":"agent_start"}` + "\n" + progressFrame("a", 1) + "not json\n" + progressFrame("a", 2) + `{"type":"tool_execution_end","toolCallId":"a"}` + "\n" + `{"type":"agent_end"`
	for i := 0; i < len(input); i += 7 {
		end := min(i+7, len(input))
		w.Write([]byte(input[i:end]))
		if out.Len() > 0 && out.Bytes()[out.Len()-1] != '\n' {
			t.Fatalf("persisted a partial frame mid-stream: %q", out.String())
		}
	}
	w.Close()
	if out.String() != input {
		t.Fatalf("frames changed:\n got %q\nwant %q", out.String(), input)
	}
}

func TestCaptureWriterFailureIsSticky(t *testing.T) {
	w, _ := newTestCaptureWriter(failingWriter{}, time.Second)
	if _, err := w.Write([]byte(`{"type":"agent_start"}` + "\n")); err == nil {
		t.Fatal("file write failure was not reported")
	}
	if _, err := w.Write([]byte(`{"type":"agent_start"}` + "\n")); err == nil {
		t.Fatal("failure was not sticky")
	}
	if err := w.Close(); err == nil {
		t.Fatal("close hid the failure")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestCaptureCoalesceOffSwitchIsByteExact(t *testing.T) {
	t.Setenv("SUPERVISOR_CAPTURE_COALESCE_MS", "0")
	var input strings.Builder
	for i := range 40 {
		input.WriteString(progressFrame("a", i))
		input.WriteString(streamFrame("a", i))
	}
	input.WriteString(`{"type":"agent_end","messages":[]}`)
	if got := runReadLoop(t, input.String()); string(got) != input.String() {
		t.Fatal("coalescing off did not keep a byte-exact capture")
	}
}

func TestCaptureCoalesceWindowEnv(t *testing.T) {
	for raw, want := range map[string]time.Duration{
		"": time.Second, "0": 0, "250": 250 * time.Millisecond, "-1": time.Second, "x": time.Second, "60001": time.Second,
	} {
		t.Setenv("SUPERVISOR_CAPTURE_COALESCE_MS", raw)
		if got := captureCoalesceWindow(); got != want {
			t.Errorf("%q: window %v, want %v", raw, got, want)
		}
	}
}

func TestCaptureReadLoopHandlesEveryFrameBeforeCoalescing(t *testing.T) {
	t.Setenv("SUPERVISOR_CAPTURE_COALESCE_MS", "60000")
	path := filepath.Join(t.TempDir(), "child.log")
	logFile, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	job := &Job{ID: "sub_seen", Status: "running"}
	child := &rpcChild{job: job, logFile: logFile, terminal: make(chan struct{}, 1), readDone: make(chan struct{})}
	reader, writer := io.Pipe()
	go child.readLoop(reader)
	io.WriteString(writer, progressFrame("a", 1)+progressFrame("a", 2))

	io.WriteString(writer, `{"type":"tool_execution_start","toolCallId":"b","toolName":"probe"}`+"\n")
	deadline := time.Now().Add(5 * time.Second)
	for {
		job.mu.RLock()
		last := job.LastTool
		job.mu.RUnlock()
		if last == "probe" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("handleFrame did not see a frame while capture held progress")
		}
		time.Sleep(5 * time.Millisecond)
	}
	writer.Close()
	<-child.readDone
	got, _ := os.ReadFile(path)
	want := progressFrame("a", 1) + `{"type":"tool_execution_start","toolCallId":"b","toolName":"probe"}` + "\n" + progressFrame("a", 2)
	if string(got) != want {
		t.Fatalf("capture = %s", got)
	}
}

func TestCursorPagingOverCoalescedCapture(t *testing.T) {
	var out bytes.Buffer
	w, clock := newTestCaptureWriter(&out, time.Second)
	w.Write([]byte(`{"type":"agent_start"}` + "\n" + `{"type":"tool_execution_start","toolCallId":"a","toolName":"task"}` + "\n"))
	for i := range 30 {
		clock.Advance(150 * time.Millisecond)
		w.Write([]byte(progressFrame("a", i)))
	}
	w.Write([]byte(`{"type":"tool_execution_end","toolCallId":"a","toolName":"task"}` + "\n" + `{"type":"agent_end","messages":[]}` + "\n"))
	w.Close()
	path := filepath.Join(t.TempDir(), "coalesced.log")
	if err := os.WriteFile(path, out.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	all, err := readJobEvents(context.Background(), path, 0, 500)
	if err != nil || all.HasMore || all.NextCursor != int64(out.Len()) {
		t.Fatalf("full read: %+v %v", all, err)
	}
	var paged []jobEvent
	var cursor int64
	for {
		page, err := readJobEvents(context.Background(), path, cursor, 2)
		if err != nil || page.Reset {
			t.Fatalf("page at %d: %+v %v", cursor, page, err)
		}
		paged = append(paged, page.Events...)
		cursor = page.NextCursor
		if !page.HasMore {
			break
		}
	}
	if len(paged) != len(all.Events) || cursor != all.NextCursor {
		t.Fatalf("paging returned %d events to %d, want %d to %d", len(paged), cursor, len(all.Events), all.NextCursor)
	}
	for i := range paged {
		if paged[i].Cursor != all.Events[i].Cursor || !bytes.Equal(paged[i].Event, all.Events[i].Event) {
			t.Fatalf("event %d differs", i)
		}
		if out.Bytes()[paged[i].Cursor-1] != '\n' {
			t.Fatalf("cursor %d is not a frame boundary", paged[i].Cursor)
		}
	}
}

func writeIndexedGzip(t *testing.T, path string, body []byte) {
	t.Helper()
	var buf bytes.Buffer
	if _, _, err := writeCaptureGzip(&buf, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCompressedCaptureSeeksByMemberIndex(t *testing.T) {
	old := captureGzipChunk
	captureGzipChunk = 64
	t.Cleanup(func() { captureGzipChunk = old })
	var body strings.Builder
	for i := range 40 {
		body.WriteString(progressFrame(fmt.Sprintf("c%d", i), i))
	}
	logPath := filepath.Join(t.TempDir(), "sub_seek.log")
	writeIndexedGzip(t, logPath+".gz", []byte(body.String()))
	capture, err := openCapture(logPath)
	if err != nil {
		t.Fatal(err)
	}
	g := capture.(*gzipCapture)
	if len(g.members) < 10 || g.Size() != int64(body.Len()) {
		t.Fatalf("members %d size %d", len(g.members), g.Size())
	}
	for _, off := range []int64{0, 1, 63, 64, 65, 1000, int64(body.Len()) - 1, int64(body.Len())} {
		reader, err := capture.ReaderFrom(off)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(reader)
		if err != nil || string(got) != body.String()[off:] {
			t.Fatalf("offset %d read %d bytes, err %v", off, len(got), err)
		}
	}
	capture.Close()

	file, _ := os.Open(logPath + ".gz")
	defer file.Close()
	all, err := io.ReadAll(mustGzipReader(t, file))
	if err != nil || string(all) != body.String() {
		t.Fatalf("multi-member gzip is not a standard stream: %v", err)
	}

	plain := filepath.Join(t.TempDir(), "plain.log")
	os.WriteFile(plain, []byte(body.String()), 0600)
	var cursor int64
	for {
		a, errA := readJobEvents(context.Background(), logPath, cursor, 3)
		b, errB := readJobEvents(context.Background(), plain, cursor, 3)
		if errA != nil || errB != nil {
			t.Fatal(errA, errB)
		}
		aj, _ := json.Marshal(a)
		bj, _ := json.Marshal(b)
		if !bytes.Equal(aj, bj) {
			t.Fatalf("compressed page at %d differs:\n%s\n%s", cursor, aj, bj)
		}
		cursor = a.NextCursor
		if !a.HasMore {
			break
		}
	}
}

func TestPlainCaptureWinsOverCompressedCopy(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "sub_both.log")
	os.WriteFile(logPath, []byte(`{"type":"agent_start"}`+"\n"), 0600)
	writeIndexedGzip(t, logPath+".gz", []byte(`{"type":"agent_end","messages":[]}`+"\n"))
	page, err := readJobEvents(context.Background(), logPath, 0, 10)
	if err != nil || len(page.Events) != 1 || !strings.Contains(string(page.Events[0].Event), "agent_start") {
		t.Fatalf("read %+v %v", page, err)
	}
}

func compressionStore(t *testing.T) *JobStore {
	t.Helper()
	oldStore := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = oldStore })
	return store
}

func TestCompressFinishedCaptureReplacesLogAndKeepsCursors(t *testing.T) {
	store := compressionStore(t)
	logPath := filepath.Join(store.logsDir, "sub_done.log")
	body := `{"type":"agent_start"}` + "\n" + progressFrame("a", 1) + `{"type":"agent_end","messages":[]}` + "\n"
	os.WriteFile(logPath, []byte(body), 0600)
	done := make(chan struct{})
	close(done)
	job := &Job{ID: "sub_done", Status: "done", Steerable: true, LogPath: logPath, captureDone: done}
	store.add(job)
	before, _ := readJobEvents(context.Background(), logPath, 0, 100)
	if err := compressFinishedCapture(store, job); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(logPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("raw capture kept: %v", err)
	}
	after, err := readJobEvents(context.Background(), logPath, 0, 100)
	a, _ := json.Marshal(before)
	b, _ := json.Marshal(after)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("events changed after compression: %v\n%s\n%s", err, a, b)
	}
	entries, _ := os.ReadDir(store.logsDir)
	if len(entries) != 1 || entries[0].Name() != "sub_done.log.gz" {
		t.Fatalf("logs dir = %v", entries)
	}
	if sum, size, compressed, err := hashLocalCapture(logPath); err != nil || !compressed || size != int64(len(body)) || sum == "" {
		t.Fatalf("hashLocalCapture = %s %d %v %v", sum, size, compressed, err)
	}
}

func TestCompressRefusesActiveCapture(t *testing.T) {
	store := compressionStore(t)
	body := []byte(`{"type":"agent_start"}` + "\n")
	cases := map[string]*Job{
		"running":    {ID: "sub_running", Status: "running", Steerable: true, captureDone: closedChan()},
		"draining":   {ID: "sub_draining", Status: "done", Steerable: true, captureDone: make(chan struct{})},
		"rpc linked": {ID: "sub_linked", Status: "done", Steerable: true, captureDone: closedChan(), rpc: &rpcChild{}},
	}
	for name, job := range cases {
		job.LogPath = filepath.Join(store.logsDir, job.ID+".log")
		os.WriteFile(job.LogPath, body, 0600)
		store.add(job)
		if err := compressFinishedCapture(store, job); err == nil || !strings.Contains(err.Error(), "still active") {
			t.Errorf("%s: compression not refused: %v", name, err)
		}
		if got, err := os.ReadFile(job.LogPath); err != nil || !bytes.Equal(got, body) {
			t.Errorf("%s: raw capture changed: %v", name, err)
		}
		if _, err := os.Stat(job.LogPath + ".gz"); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s: compressed copy written", name)
		}
	}
	compressFinishedCaptures(store)
	for name, job := range cases {
		if _, err := os.Stat(job.LogPath); err != nil {
			t.Errorf("%s: sweep compressed an active capture: %v", name, err)
		}
	}
}

func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func TestStartupSweepCompressesOnlyFinishedCaptures(t *testing.T) {
	base := t.TempDir()
	store, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"type":"agent_start"}` + "\n" + `{"type":"tool_execution_start","toolCallId":"a","toolName":"replayed_tool"}` + "\n")
	for _, job := range []*Job{
		{ID: "sub_old_done", Status: "done", Steerable: true},
		{ID: "sub_old_print", Status: "done"},
		{ID: "sub_still_live", Status: "running", Steerable: true, PID: os.Getpid()},
	} {
		job.LogPath = filepath.Join(store.logsDir, job.ID+".log")
		os.WriteFile(job.LogPath, body, 0600)
		store.add(job)
	}
	t.Setenv("SUPERVISOR_CAPTURE_COMPRESS", "off")
	compressFinishedCaptures(store)
	if _, err := os.Stat(filepath.Join(store.logsDir, "sub_old_done.log")); err != nil {
		t.Fatal("compression ran while disabled")
	}
	t.Setenv("SUPERVISOR_CAPTURE_COMPRESS", "")

	restarted, err := newJobStore(base)
	if err != nil {
		t.Fatal(err)
	}
	compressFinishedCaptures(restarted)
	if _, err := os.Stat(filepath.Join(store.logsDir, "sub_old_done.log.gz")); err != nil {
		t.Fatalf("finished capture not compressed: %v", err)
	}
	for _, id := range []string{"sub_old_print", "sub_still_live"} {
		if _, err := os.Stat(filepath.Join(store.logsDir, id+".log")); err != nil {
			t.Fatalf("%s: capture touched: %v", id, err)
		}
	}

	job := restarted.get("sub_old_done")
	job.mu.Lock()
	job.LastTool = ""
	job.needsMetadataReplay = true
	job.mu.Unlock()
	restarted.ensureRecoveredMetadata(job)
	job.mu.RLock()
	lastTool := job.LastTool
	job.mu.RUnlock()
	if lastTool != "replayed_tool" {
		t.Fatalf("metadata replay did not read the compressed capture: %q", lastTool)
	}
}

func mustGzipReader(t *testing.T, r io.Reader) io.Reader {
	t.Helper()
	zr, err := gzip.NewReader(bufio.NewReader(r))
	if err != nil {
		t.Fatal(err)
	}
	return zr
}

func TestAckArchivesAndRestoresCompressedCapture(t *testing.T) {
	newTestStores(t)
	restoreArchiveHooks(t)
	mem := &memArchive{}
	openArchiveTransport = func(spec archiveTarget) (archiveTransport, error) { return mem, nil }
	t.Setenv("SUPERVISOR_CAPTURE_ARCHIVE_TARGET", "archive@archive.example:/var/lib/supervisor-archive")
	job, logPath := readyCaptureJob(t, "sub_ack_gz")
	body := []byte(`{"type":"agent_start"}` + "\n" + progressFrame("a", 1))
	if err := os.WriteFile(logPath, body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := compressFinishedCapture(globalStore, job); err != nil {
		t.Fatal(err)
	}
	res, payload := callTool(t, handleAckCapture, ackArgs(job))
	if res.IsError || payload["capture_acknowledged"] != true {
		t.Fatalf("ACK of a compressed capture failed: %v", payload)
	}
	job.mu.RLock()
	archive := job.CaptureArchive
	job.mu.RUnlock()
	if archive == nil || !archive.LocalRemoved {
		t.Fatalf("compressed capture was not archived: %#v", archive)
	}
	if _, err := os.Stat(logPath + ".gz"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("local compressed capture kept after verified archive: %v", err)
	}
	if got := gunzipBytes(t, mem.must(t, job.ID+".log.gz")); !bytes.Equal(got, body) {
		t.Fatalf("remote log = %q", got)
	}
	res, payload = callTool(t, handleRestoreCapture, map[string]any{"job_id": job.ID})
	if res.IsError || payload["restored"] != true {
		t.Fatalf("restore failed: %v", payload)
	}
	if got, err := os.ReadFile(logPath); err != nil || !bytes.Equal(got, body) {
		t.Fatalf("restored log = %q err=%v", got, err)
	}
}
