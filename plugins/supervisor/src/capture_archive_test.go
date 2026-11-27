package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseArchiveTargetRejectsForbiddenOverlap(t *testing.T) {
	spec, err := parseArchiveTarget("archiver@archive.example:/srv/archive")
	if err != nil {
		t.Fatal(err)
	}
	if spec.captures != "/srv/archive/supervisor-captures" {
		t.Fatalf("captures root = %s", spec.captures)
	}
	t.Setenv("SUPERVISOR_CAPTURE_FORBIDDEN", "/srv/archive/protected")
	forbiddenCaptureArchive = os.Getenv("SUPERVISOR_CAPTURE_FORBIDDEN")
	t.Cleanup(func() { forbiddenCaptureArchive = "" })
	for _, raw := range []string{
		"archiver@archive.example:/srv/archive/protected",
		"archiver@archive.example:/srv/archive/protected/nested",
		"archiver@host:/tmp/../etc",
		"not a target",
	} {
		if _, err := parseArchiveTarget(raw); err == nil {
			t.Fatalf("accepted unsafe target %s", raw)
		}
	}
}

func TestCaptureRetentionRemovesOnlyExpiredDateDirs(t *testing.T) {
	mem := &memArchive{dates: []string{"2000-01-01", "not-a-date", "../etc", time.Now().UTC().Format("2006-01-02")}}
	restoreArchiveHooks(t)
	openArchiveTransport = func(spec archiveTarget) (archiveTransport, error) { return mem, nil }
	t.Setenv("SUPERVISOR_CAPTURE_ARCHIVE_TARGET", "archive@archive.example:/var/lib/supervisor-archive")
	runCaptureRetention()
	if len(mem.removed) != 1 || !strings.HasSuffix(mem.removed[0], "/supervisor-captures/2000-01-01") {
		t.Fatalf("removed = %#v", mem.removed)
	}
}

func TestAckRefusesMismatchActiveAndCaptureError(t *testing.T) {
	newTestStores(t)
	restoreArchiveHooks(t)
	openArchiveTransport = func(spec archiveTarget) (archiveTransport, error) {
		t.Fatal("transport opened for a refused ack")
		return nil, nil
	}
	job, logPath := readyCaptureJob(t, "sub_ack_refuse")
	if err := os.WriteFile(logPath, []byte("{\"type\":\"agent_start\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}

	mismatch := ackArgs(job)
	mismatch["parent_session_id"] = "other-parent"
	res, payload := callTool(t, handleAckCapture, mismatch)
	if !res.IsError || !strings.Contains(payload["__error_text"].(string), "does not match") {
		t.Fatalf("mismatch accepted: %v", payload)
	}
	if job.CaptureAcknowledged {
		t.Fatal("mismatch persisted an ack")
	}

	job.mu.Lock()
	job.Status = "running"
	job.captureDone = make(chan struct{})
	job.mu.Unlock()
	res, payload = callTool(t, handleAckCapture, ackArgs(job))
	if !res.IsError || !strings.Contains(payload["__error_text"].(string), "not terminal") {
		t.Fatalf("running job acknowledged: %v", payload)
	}

	job.mu.Lock()
	job.Status = "done"
	job.CaptureError = "stream broke"
	job.mu.Unlock()
	res, payload = callTool(t, handleAckCapture, ackArgs(job))
	if !res.IsError || !strings.Contains(payload["__error_text"].(string), "capture_error") {
		t.Fatalf("capture_error acknowledged: %v", payload)
	}

	job.mu.Lock()
	job.CaptureError = ""
	job.Steerable = false
	close(job.captureDone)
	job.mu.Unlock()
	res, payload = callTool(t, handleAckCapture, ackArgs(job))
	if !res.IsError || !strings.Contains(payload["__error_text"].(string), "not steerable") {
		t.Fatalf("print job acknowledged: %v", payload)
	}
	if _, err := os.Stat(logPath); err != nil {
		t.Fatal("refused ack removed the raw log")
	}
}

func TestAckRefusesUndrainedCapture(t *testing.T) {
	newTestStores(t)
	restoreArchiveHooks(t)
	job, logPath := readyCaptureJob(t, "sub_ack_drain")
	if err := os.WriteFile(logPath, []byte("{\"type\":\"agent_start\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	job.mu.Lock()
	job.captureDone = make(chan struct{})
	job.mu.Unlock()
	res, payload := callTool(t, handleAckCapture, ackArgs(job))
	if !res.IsError || !strings.Contains(payload["__error_text"].(string), "draining") {
		t.Fatalf("open capture acknowledged: %v", payload)
	}
	if job.CaptureAcknowledged {
		t.Fatal("undrained capture persisted an ack")
	}
}

func TestAckArchiveFailureRetainsLocalLog(t *testing.T) {
	store := testStore(t)
	restoreArchiveHooks(t)
	mem := &memArchive{failPut: true}
	openArchiveTransport = func(spec archiveTarget) (archiveTransport, error) { return mem, nil }
	t.Setenv("SUPERVISOR_CAPTURE_ARCHIVE_TARGET", "archive@archive.example:/var/lib/supervisor-archive")
	job, logPath := readyCaptureJob(t, "sub_ack_fail")
	body := []byte("{\"type\":\"agent_start\"}\n")
	if err := os.WriteFile(logPath, body, 0600); err != nil {
		t.Fatal(err)
	}
	res, payload := callTool(t, handleAckCapture, ackArgs(job))
	if res.IsError || payload["capture_acknowledged"] != true || payload["archived"] != false {
		t.Fatalf("ACK depended on optional archive: %v", payload)
	}
	got, err := os.ReadFile(logPath)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("archive failure did not retain local log: %q err=%v", got, err)
	}
	job.mu.RLock()
	durable, archive := job.ackDurable, job.CaptureArchive
	job.mu.RUnlock()
	if !durable || archive != nil {
		t.Fatalf("archive failure state = durable %v archive %v", durable, archive)
	}
	if _, err := os.Stat(filepath.Join(store.dataDir, job.ID+".json")); err != nil {
		t.Fatal(err)
	}
}

func TestAckPersistenceFailureRetainsLocalLog(t *testing.T) {
	store := testStore(t)
	restoreArchiveHooks(t)
	t.Setenv("SUPERVISOR_CAPTURE_ARCHIVE_TARGET", "archive@archive.example:/var/lib/supervisor-archive")
	job, logPath := readyCaptureJob(t, "sub_ack_meta")
	body := []byte("{\"type\":\"agent_start\",\"id\":\"keep-local\"}\n")
	if err := os.WriteFile(logPath, body, 0600); err != nil {
		t.Fatal(err)
	}
	started := false
	launchCaptureArchive = func(work func()) { started = true }
	persistJobHook = func(j *Job) error {
		j.mu.RLock()
		defer j.mu.RUnlock()
		if j.CaptureAck != nil {
			return errors.New("disk full")
		}
		return nil
	}
	res, payload := callTool(t, handleAckCapture, ackArgs(job))
	if !res.IsError || !strings.Contains(payload["__error_text"].(string), "ack was not saved") {
		t.Fatalf("ACK persistence failure was accepted: %v", payload)
	}
	if started || job.ackDurable || job.CaptureAcknowledged {
		t.Fatal("failed ACK became durable")
	}
	got, err := os.ReadFile(logPath)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("ACK persistence failure removed local log: %q err=%v", got, err)
	}
	if _, err := os.Stat(filepath.Join(store.dataDir, job.ID+".json")); err != nil {
		t.Fatal(err)
	}
}

func TestAckVerifiedArchiveThenRestoreReplay(t *testing.T) {
	newTestStores(t)
	restoreArchiveHooks(t)
	mem := &memArchive{}
	openArchiveTransport = func(spec archiveTarget) (archiveTransport, error) { return mem, nil }
	t.Setenv("SUPERVISOR_CAPTURE_ARCHIVE_TARGET", "archive@archive.example:/var/lib/supervisor-archive")
	job, logPath := readyCaptureJob(t, "sub_ack_ok")
	body := []byte("{\"type\":\"agent_start\",\"id\":\"replay-marker\"}\n")
	stderrBody := []byte("child warning\n")
	if err := os.WriteFile(logPath, body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath+".stderr", stderrBody, 0600); err != nil {
		t.Fatal(err)
	}
	res, payload := callTool(t, handleAckCapture, ackArgs(job))
	if res.IsError || payload["capture_acknowledged"] != true {
		t.Fatalf("ACK failed: %v", payload)
	}
	job.mu.RLock()
	ackSaved := job.CaptureAck != nil
	archive := job.CaptureArchive
	job.mu.RUnlock()
	if !ackSaved {
		t.Fatal("ACK did not store capture_ack before archiving")
	}
	if archive == nil || !archive.LocalRemoved {
		t.Fatalf("archive worker did not durably remove verified capture: %#v", archive)
	}
	if _, err := os.Stat(logPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("local log still present: %v", err)
	}
	if _, err := os.Stat(logPath + ".stderr"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("local stderr still present: %v", err)
	}
	if got := gunzipBytes(t, mem.must(t, job.ID+".log.gz")); !bytes.Equal(got, body) {
		t.Fatalf("remote log = %q", got)
	}
	rawSum := sha256.Sum256(body)
	if !bytes.Contains(mem.must(t, "manifest.json"), []byte(hex.EncodeToString(rawSum[:]))) {
		t.Fatal("manifest missing original checksum")
	}
	puts := mem.uploads()
	res, payload = callTool(t, handleAckCapture, ackArgs(job))
	if res.IsError || payload["capture_acknowledged"] != true || mem.uploads() != puts {
		t.Fatalf("repeat ACK re-uploaded or failed: uploads %d->%d %v", puts, mem.uploads(), payload)
	}
	other := ackArgs(job)
	other["session_id"] = "other-child"
	res, _ = callTool(t, handleAckCapture, other)
	if !res.IsError {
		t.Fatal("mismatched ACK identity was accepted")
	}
	res, payload = callTool(t, handleRestoreCapture, map[string]any{"job_id": job.ID})
	if res.IsError || payload["restored"] != true {
		t.Fatalf("restore failed: %v", payload)
	}
	for localPath, expected := range map[string][]byte{logPath: body, logPath + ".stderr": stderrBody} {
		got, err := os.ReadFile(localPath)
		if err != nil || !bytes.Equal(got, expected) {
			t.Fatalf("restored %s = %q err=%v", localPath, got, err)
		}
	}
	job.mu.RLock()
	intent := job.CaptureArchive.RestoreIntent
	removed := job.CaptureArchive.LocalRemoved
	localRestored := job.CaptureArchive.LocalRestored
	job.mu.RUnlock()
	if intent || removed || !localRestored {
		t.Fatalf("restored metadata = intent %v removed %v restored %v", intent, removed, localRestored)
	}
	res, _ = callTool(t, handleAckCapture, ackArgs(job))
	if res.IsError {
		t.Fatal("repeated ACK after restore failed")
	}
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("repeated ACK removed explicitly restored log: %v", err)
	}
	res, page := callTool(t, handleEvents, map[string]any{"job_id": job.ID})
	if res.IsError || page["next_cursor"] != float64(len(body)) {
		t.Fatalf("events did not replay exact restored bytes: %v", page)
	}
}

func TestAckWritesCaptureAckToDisk(t *testing.T) {
	store := testStore(t)
	restoreArchiveHooks(t)
	job, logPath := readyCaptureJob(t, "sub_ack_disk")
	if err := os.WriteFile(logPath, []byte("{\"type\":\"agent_start\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if res, payload := callTool(t, handleAckCapture, ackArgs(job)); res.IsError {
		t.Fatalf("ACK failed: %v", payload)
	}
	raw, err := os.ReadFile(filepath.Join(store.dataDir, job.ID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	ack, ok := record["capture_ack"].(map[string]any)
	if !ok || ack["session_id"] != job.SessionID || ack["acknowledged_at"] == nil {
		t.Fatalf("durable record lacks capture_ack identity: %v", record["capture_ack"])
	}
}

func TestRestoreDoesNotClobberDifferentLocalFile(t *testing.T) {
	newTestStores(t)
	restoreArchiveHooks(t)
	mem := &memArchive{}
	openArchiveTransport = func(spec archiveTarget) (archiveTransport, error) { return mem, nil }
	t.Setenv("SUPERVISOR_CAPTURE_ARCHIVE_TARGET", "archive@archive.example:/var/lib/supervisor-archive")
	job, logPath := readyCaptureJob(t, "sub_restore_keep")
	if err := os.WriteFile(logPath, []byte("{\"type\":\"agent_start\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if res, payload := callTool(t, handleAckCapture, ackArgs(job)); res.IsError {
		t.Fatal(payload)
	}
	if res, payload := callTool(t, handleRestoreCapture, map[string]any{"job_id": job.ID}); res.IsError {
		t.Fatal(payload)
	}
	different := []byte("{\"type\":\"agent_start\",\"id\":\"local-only\"}\n")
	if err := os.WriteFile(logPath, different, 0600); err != nil {
		t.Fatal(err)
	}
	logRemote := archiveRemotePath(t, job, "log")
	fetched := mem.fetches(logRemote)
	res, payload := callTool(t, handleRestoreCapture, map[string]any{"job_id": job.ID})
	if !res.IsError || !strings.Contains(payload["__error_text"].(string), "differs") {
		t.Fatalf("different local file was replaced: %v", payload)
	}
	got, err := os.ReadFile(logPath)
	if err != nil || !bytes.Equal(got, different) {
		t.Fatalf("local file changed: %q err=%v", got, err)
	}
	if mem.fetches(logRemote) != fetched {
		t.Fatal("mismatched local file's archive was downloaded")
	}
}

func TestRestoreRefusesTamperedManifestAndWritesNothing(t *testing.T) {
	newTestStores(t)
	restoreArchiveHooks(t)
	mem := &memArchive{}
	openArchiveTransport = func(spec archiveTarget) (archiveTransport, error) { return mem, nil }
	t.Setenv("SUPERVISOR_CAPTURE_ARCHIVE_TARGET", "archive@archive.example:/var/lib/supervisor-archive")
	job, logPath := readyCaptureJob(t, "sub_restore_tamper")
	if err := os.WriteFile(logPath, []byte("{\"type\":\"agent_start\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if res, payload := callTool(t, handleAckCapture, ackArgs(job)); res.IsError {
		t.Fatal(payload)
	}
	if _, err := os.Stat(logPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verified ACK did not remove local log before restore: %v", err)
	}
	mem.corrupt("manifest.json")
	res, payload := callTool(t, handleRestoreCapture, map[string]any{"job_id": job.ID})
	if !res.IsError {
		t.Fatalf("tampered manifest restored: %v", payload)
	}
	if _, err := os.Stat(logPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("refused restore wrote the local log: %v", err)
	}
	job.mu.RLock()
	intent := job.CaptureArchive.RestoreIntent
	job.mu.RUnlock()
	if intent {
		t.Fatal("refused restore left a restore intent behind")
	}
}

func TestCaptureRetentionDaysFallsBack(t *testing.T) {
	t.Setenv("SUPERVISOR_CAPTURE_ARCHIVE_RETENTION_DAYS", "")
	if captureRetentionDays() != 30 {
		t.Fatal("default retention was not 30")
	}
	t.Setenv("SUPERVISOR_CAPTURE_ARCHIVE_RETENTION_DAYS", "14")
	if captureRetentionDays() != 14 {
		t.Fatal("explicit retention was not kept")
	}
	for _, raw := range []string{"0", "366", "nope", "-3"} {
		t.Setenv("SUPERVISOR_CAPTURE_ARCHIVE_RETENTION_DAYS", raw)
		if captureRetentionDays() != 30 {
			t.Fatalf("%s did not fall back to 30", raw)
		}
	}
}

func testStore(t *testing.T) *JobStore {
	t.Helper()
	newTestStores(t)
	return globalStore
}

func restoreArchiveHooks(t *testing.T) {
	t.Helper()
	prevTransport := openArchiveTransport
	prevHook := persistJobHook
	prevWorker := runCaptureArchive
	prevWait := archiveRetryWait
	prevLaunch := launchCaptureArchive
	t.Cleanup(func() {
		openArchiveTransport = prevTransport
		persistJobHook = prevHook
		runCaptureArchive = prevWorker
		archiveRetryWait = prevWait
		launchCaptureArchive = prevLaunch
	})

	launchCaptureArchive = func(work func()) { work() }
	archiveRetryWait = func(time.Duration) {}
}

func readyCaptureJob(t *testing.T, id string) (*Job, string) {
	t.Helper()
	logPath := filepath.Join(globalStore.logsDir, id+".log")
	done := make(chan struct{})
	close(done)
	job := &Job{
		ID: id, Status: "done", Steerable: true, LogPath: logPath, captureDone: done,
		SessionID: "child-sess", ParentSessionID: "parent-sess", RootSessionID: "root-sess",
		ParentAgentID: "parent-agent", ParentToolCallID: "tool-call",
	}
	globalStore.add(job)
	return job, logPath
}

func ackArgs(job *Job) map[string]any {
	return map[string]any{
		"job_id": job.ID, "parent_session_id": job.ParentSessionID,
		"root_session_id": job.RootSessionID, "parent_agent_id": job.ParentAgentID,
		"parent_tool_call_id": job.ParentToolCallID, "session_id": job.SessionID,
	}
}

func archiveRemotePath(t *testing.T, job *Job, kind string) string {
	t.Helper()
	job.mu.RLock()
	defer job.mu.RUnlock()
	if job.CaptureArchive == nil {
		t.Fatal("job has no archive metadata")
	}
	for _, obj := range job.CaptureArchive.Files {
		if obj.Kind == kind {
			return obj.RemotePath
		}
	}
	t.Fatalf("no %s object recorded", kind)
	return ""
}

func gunzipBytes(t *testing.T, body []byte) []byte {
	t.Helper()
	r, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

type memArchive struct {
	mu      sync.Mutex
	files   map[string][]byte
	fetched map[string]int
	dates   []string
	removed []string
	puts    int
	failPut bool
}

func (m *memArchive) uploads() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.puts
}

func (m *memArchive) fetches(remotePath string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fetched[remotePath]
}

func (m *memArchive) Put(remoteDir, finalName, localPath string) (string, int64, error) {
	body, err := os.ReadFile(localPath)
	if err != nil {
		return "", 0, err
	}
	sum := sha256.Sum256(body)
	hexSum := hex.EncodeToString(sum[:])
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failPut {
		return "", 0, errors.New("ssh put failed")
	}
	if m.files == nil {
		m.files = map[string][]byte{}
	}
	key := path.Join(remoteDir, finalName)
	if have, ok := m.files[key]; ok {
		got := sha256.Sum256(have)
		if hex.EncodeToString(got[:]) != hexSum || int64(len(have)) != int64(len(body)) {
			return "", 0, errors.New("remote object does not match")
		}
		return hexSum, int64(len(body)), nil
	}
	m.files[key] = append([]byte(nil), body...)
	m.puts++
	return hexSum, int64(len(body)), nil
}

func (m *memArchive) Stat(remotePath string) (string, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	body, ok := m.files[remotePath]
	if !ok {
		return "", 0, errArchiveNotFound
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), int64(len(body)), nil
}

func (m *memArchive) Get(remotePath, localPath string) error {
	m.mu.Lock()
	body, ok := m.files[remotePath]
	if m.fetched == nil {
		m.fetched = map[string]int{}
	}
	m.fetched[remotePath]++
	m.mu.Unlock()
	if !ok {
		return errArchiveNotFound
	}
	return os.WriteFile(localPath, body, 0600)
}

func (m *memArchive) ListDates(capturesRoot string) ([]string, error) {
	if path.Base(capturesRoot) != "supervisor-captures" {
		return nil, errors.New("outside supervisor-captures")
	}
	return append([]string(nil), m.dates...), nil
}

func (m *memArchive) RemoveDate(dateDir string) error {
	if path.Base(path.Dir(dateDir)) != "supervisor-captures" {
		return errors.New("refusing sibling delete")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removed = append(m.removed, dateDir)
	return nil
}

func (m *memArchive) must(t *testing.T, suffix string) []byte {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, body := range m.files {
		if strings.HasSuffix(key, "/"+suffix) {
			return append([]byte(nil), body...)
		}
	}
	t.Fatalf("missing remote %s", suffix)
	return nil
}

func (m *memArchive) corrupt(suffix string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, body := range m.files {
		if strings.HasSuffix(key, "/"+suffix) && len(body) > 0 {
			body[0] ^= 0xff
		}
	}
}
