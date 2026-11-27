package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

//go:embed scripts/capture-archive-remote.sh
var remoteArchiveScript string

var persistJobHook func(*Job) error

var forbiddenCaptureArchive = os.Getenv("SUPERVISOR_CAPTURE_FORBIDDEN")

var errArchiveNotFound = errors.New("archive object not found")

type CaptureAck struct {
	JobID            string    `json:"job_id"`
	ParentSessionID  string    `json:"parent_session_id"`
	RootSessionID    string    `json:"root_session_id"`
	ParentAgentID    string    `json:"parent_agent_id"`
	ParentToolCallID string    `json:"parent_tool_call_id"`
	SessionID        string    `json:"session_id"`
	AcknowledgedAt   time.Time `json:"acknowledged_at"`
}

type ArchiveObject struct {
	Kind             string `json:"kind"`
	LocalPath        string `json:"local_path,omitempty"`
	RemotePath       string `json:"remote_path"`
	OriginalSHA256   string `json:"original_sha256"`
	OriginalSize     int64  `json:"original_size"`
	CompressedSHA256 string `json:"compressed_sha256"`
	CompressedSize   int64  `json:"compressed_size"`
}

type CaptureArchive struct {
	ArchivedAt    time.Time       `json:"archived_at"`
	Date          string          `json:"date"`
	Host          string          `json:"host"`
	Root          string          `json:"root"`
	RemoteDir     string          `json:"remote_dir"`
	Files         []ArchiveObject `json:"files"`
	ManifestPath  string          `json:"manifest_path"`
	ManifestSHA   string          `json:"manifest_sha256"`
	RestoreIntent bool            `json:"restore_intent,omitempty"`
	LocalRestored bool            `json:"local_restored,omitempty"`
	LocalRemoved  bool            `json:"local_removed"`
}

type archiveManifestFile struct {
	Kind             string `json:"kind"`
	RemoteName       string `json:"remote_name"`
	OriginalSHA256   string `json:"original_sha256"`
	OriginalSize     int64  `json:"original_size"`
	CompressedSHA256 string `json:"compressed_sha256"`
	CompressedSize   int64  `json:"compressed_size"`
}

type archiveManifest struct {
	JobID string                `json:"job_id"`
	Date  string                `json:"date"`
	Files []archiveManifestFile `json:"files"`
}

type archiveTarget struct {
	userHost string
	root     string
	captures string
}

type archiveTransport interface {
	Put(remoteDir, finalName, localPath string) (sha string, size int64, err error)
	Stat(remotePath string) (sha string, size int64, err error)
	Get(remotePath, localPath string) error
	ListDates(capturesRoot string) ([]string, error)
	RemoveDate(dateDir string) error
}

type sshArchive struct {
	spec archiveTarget
	ctx  context.Context
}

func archiveTransportForContext(ctx context.Context, spec archiveTarget) (archiveTransport, error) {
	transport, err := openArchiveTransport(spec)
	if err != nil {
		return nil, err
	}
	if ssh, ok := transport.(*sshArchive); ok {
		ssh.ctx = ctx
	}
	return transport, nil
}

func (s *sshArchive) requestContext() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

var openArchiveTransport = func(spec archiveTarget) (archiveTransport, error) {
	return &sshArchive{spec: spec}, nil
}

var runCaptureArchive = archiveCaptureWithRetry
var archiveRetryWait = time.Sleep
var launchCaptureArchive = func(work func()) { go work() }

func scheduleCaptureArchive(store *JobStore, job *Job) {
	if strings.TrimSpace(os.Getenv("SUPERVISOR_CAPTURE_ARCHIVE_TARGET")) == "" {
		return
	}
	job.mu.Lock()
	if !job.ackDurable || job.archiveScheduled {
		job.mu.Unlock()
		return
	}
	job.archiveScheduled = true
	job.mu.Unlock()
	worker := runCaptureArchive
	launchCaptureArchive(func() {
		defer func() {
			job.mu.Lock()
			job.archiveScheduled = false
			job.mu.Unlock()
		}()
		worker(store, job)
	})
}

func retryAcknowledgedCaptures(store *JobStore) {
	if store == nil {
		return
	}
	store.mu.RLock()
	jobs := make([]*Job, 0, len(store.jobs))
	for _, job := range store.jobs {
		job.mu.RLock()
		acknowledged := job.ackDurable && archivePending(job.CaptureArchive)
		job.mu.RUnlock()
		if acknowledged {
			jobs = append(jobs, job)
		}
	}
	store.mu.RUnlock()
	for _, job := range jobs {
		scheduleCaptureArchive(store, job)
	}
}

func archivePending(a *CaptureArchive) bool {
	return a == nil || (!a.LocalRemoved && !a.LocalRestored && !a.RestoreIntent)
}

func ackIdentityFromArgs(args map[string]any) (CaptureAck, error) {
	id := CaptureAck{
		JobID:            getStringArg(args, "job_id", ""),
		ParentSessionID:  getStringArg(args, "parent_session_id", ""),
		RootSessionID:    getStringArg(args, "root_session_id", ""),
		ParentAgentID:    getStringArg(args, "parent_agent_id", ""),
		ParentToolCallID: getStringArg(args, "parent_tool_call_id", ""),
		SessionID:        getStringArg(args, "session_id", ""),
	}
	if id.JobID == "" || id.ParentSessionID == "" || id.RootSessionID == "" || id.ParentAgentID == "" || id.ParentToolCallID == "" || id.SessionID == "" {
		return CaptureAck{}, fmt.Errorf("ack identity is incomplete")
	}
	return id, nil
}

func linkageMatches(job *Job, id CaptureAck) bool {
	return job.ID == id.JobID &&
		job.ParentSessionID == id.ParentSessionID &&
		job.RootSessionID == id.RootSessionID &&
		job.ParentAgentID == id.ParentAgentID &&
		job.ParentToolCallID == id.ParentToolCallID &&
		job.SessionID == id.SessionID
}

func sameAck(saved *CaptureAck, id CaptureAck) bool {
	if saved == nil {
		return false
	}
	return saved.JobID == id.JobID &&
		saved.ParentSessionID == id.ParentSessionID &&
		saved.RootSessionID == id.RootSessionID &&
		saved.ParentAgentID == id.ParentAgentID &&
		saved.ParentToolCallID == id.ParentToolCallID &&
		saved.SessionID == id.SessionID
}

func jobTerminal(status string) bool {
	switch status {
	case "done", "error", "canceled":
		return true
	default:
		return false
	}
}

func captureDrained(job *Job) bool {
	if job.captureDone == nil {
		return jobTerminal(job.Status)
	}
	select {
	case <-job.captureDone:
		return true
	default:
		return false
	}
}

func captureNotReady(job *Job, logRequired bool) error {
	if !job.Steerable {
		return fmt.Errorf("capture is not ready to acknowledge: job is not steerable")
	}
	if !jobTerminal(job.Status) {
		return fmt.Errorf("capture is not ready to acknowledge: job is not terminal")
	}
	if strings.TrimSpace(job.CaptureError) != "" {
		return fmt.Errorf("capture is not ready to acknowledge: capture_error")
	}
	if !captureDrained(job) {
		return fmt.Errorf("capture is not ready to acknowledge: capture is still draining")
	}
	if logRequired {
		if job.LogPath == "" || !localCapturePresent(job.LogPath) {
			return fmt.Errorf("capture is not ready to acknowledge: raw capture log is not present")
		}
	}
	return nil
}

func handleAckCapture(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	_ = ctx
	identity, err := ackIdentityFromArgs(request.GetArguments())
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if globalStore == nil {
		return mcp.NewToolResultError("job store unavailable"), nil
	}
	job := globalStore.get(identity.JobID)
	if job == nil {
		return mcp.NewToolResultError(fmt.Sprintf("job %q not found", identity.JobID)), nil
	}
	if !jobIDToken(identity.JobID) {
		return mcp.NewToolResultError("ack identity does not match job linkage"), nil
	}

	job.ackMu.Lock()
	defer job.ackMu.Unlock()

	job.mu.Lock()
	if !linkageMatches(job, identity) || (job.CaptureAcknowledged && !sameAck(job.CaptureAck, identity)) {
		job.mu.Unlock()
		return mcp.NewToolResultError("ack identity does not match job linkage"), nil
	}
	freshAck := !job.CaptureAcknowledged
	archiveState := cloneArchive(job.CaptureArchive)
	if freshAck {
		if err := captureNotReady(job, true); err != nil {
			job.mu.Unlock()
			return mcp.NewToolResultError(err.Error()), nil
		}
		ack := identity
		ack.AcknowledgedAt = time.Now().UTC()
		job.CaptureAcknowledged = true
		job.CaptureAck = &ack
	} else if err := captureNotReady(job, archiveState == nil || (!archiveState.LocalRemoved && !archiveState.LocalRestored && !archiveState.RestoreIntent)); err != nil {
		job.mu.Unlock()
		return mcp.NewToolResultError(err.Error()), nil
	}
	status := job.Status
	job.mu.Unlock()

	if freshAck {
		if err := globalStore.save(job); err != nil {
			job.mu.Lock()
			job.CaptureAcknowledged = false
			job.CaptureAck = nil
			job.mu.Unlock()
			return mcp.NewToolResultError("ack was not saved: " + err.Error()), nil
		}
		job.mu.Lock()
		job.ackDurable = true
		job.mu.Unlock()
	}
	if archiveState != nil && archiveState.LocalRestored {
		return toolJSON(ackPayload(identity, status, false)), nil
	}
	scheduleCaptureArchive(globalStore, job)
	return toolJSON(ackPayload(identity, status, archiveState != nil && archiveState.LocalRemoved)), nil

}

func ackPayload(id CaptureAck, status string, archived bool) map[string]any {
	return map[string]any{
		"job_id":               id.JobID,
		"parent_session_id":    id.ParentSessionID,
		"root_session_id":      id.RootSessionID,
		"parent_agent_id":      id.ParentAgentID,
		"parent_tool_call_id":  id.ParentToolCallID,
		"session_id":           id.SessionID,
		"status":               status,
		"capture_acknowledged": true,
		"archived":             archived,
		"local_retained":       !archived,
	}
}

func handleRestoreCapture(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	id := getStringArg(request.GetArguments(), "job_id", "")
	if id == "" || globalStore == nil {
		return mcp.NewToolResultError("job_id is required"), nil
	}
	job := globalStore.get(id)
	if job == nil {
		return mcp.NewToolResultError(fmt.Sprintf("job %q not found", id)), nil
	}
	if err := restoreJobCapture(ctx, globalStore, job); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	job.mu.RLock()
	defer job.mu.RUnlock()
	out := map[string]any{
		"job_id":   job.ID,
		"restored": true,
		"log_path": job.LogPath,
	}
	files := make([]map[string]any, 0, 2)
	if job.CaptureArchive != nil {
		for _, obj := range job.CaptureArchive.Files {
			if obj.Kind != "log" && obj.Kind != "stderr" {
				continue
			}
			files = append(files, map[string]any{
				"kind":   obj.Kind,
				"path":   obj.LocalPath,
				"sha256": obj.OriginalSHA256,
				"size":   obj.OriginalSize,
			})
		}
	}
	out["files"] = files
	return toolJSON(out), nil
}

func archiveJobCapture(ctx context.Context, store *JobStore, job *Job) error {
	job.archiveMu.Lock()
	defer job.archiveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}

	target := strings.TrimSpace(os.Getenv("SUPERVISOR_CAPTURE_ARCHIVE_TARGET"))
	if target == "" {
		return nil
	}
	spec, err := parseArchiveTarget(target)
	if err != nil {
		return err
	}
	job.mu.RLock()
	if !job.ackDurable || job.CaptureAck == nil {
		job.mu.RUnlock()
		return fmt.Errorf("capture archive refused: ack is not durable")
	}
	if err := captureNotReady(job, false); err != nil {
		job.mu.RUnlock()
		return err
	}
	existing := cloneArchive(job.CaptureArchive)
	if existing != nil && (existing.LocalRestored || existing.RestoreIntent) {
		job.mu.RUnlock()
		return nil
	}
	logPath := job.LogPath
	jobID := job.ID
	job.mu.RUnlock()

	if !jobIDToken(jobID) {
		return fmt.Errorf("capture archive refused: job id is not archive-safe")
	}
	if err := jobLogOwned(store, jobID, logPath); err != nil {
		return fmt.Errorf("capture archive refused: %w", err)
	}
	transport, err := archiveTransportForContext(ctx, spec)
	if err != nil {
		return fmt.Errorf("capture archive refused: %w", err)
	}
	if existing != nil && existing.LocalRemoved {
		if err := verifyStoredArchive(ctx, spec, transport, existing); err != nil {
			return fmt.Errorf("capture archive refused: %w", err)
		}
		return nil
	}
	if existing != nil && existing.RemoteDir != "" {
		if err := verifyStoredArchive(ctx, spec, transport, existing); err != nil {
			return fmt.Errorf("capture archive refused: %w", err)
		}
		if err := saveArchiveMetadata(store, job, existing); err != nil {
			return err
		}
		return removeVerifiedLocals(store, job, existing, false)
	}

	date := time.Now().UTC().Format("2006-01-02")
	remoteDir := path.Join(spec.captures, date, jobID)
	if err := validateRemotePath(remoteDir); err != nil || !hasPathPrefix(remoteDir, spec.captures) || !provenDisjoint(remoteDir, forbiddenCaptureArchive) {
		return fmt.Errorf("capture archive refused: remote directory is not proven safe")
	}
	stderrPath := logPath + ".stderr"
	hadStderr := false
	if info, err := os.Lstat(stderrPath); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("capture archive refused: stderr capture is not a regular file")
		}
		hadStderr = true
	}
	objects := make([]ArchiveObject, 0, 3)
	logObj, err := uploadCapture(transport, remoteDir, jobID+".log.gz", logPath, "log", true)
	if err != nil {
		return fmt.Errorf("capture archive failed: %w", err)
	}
	objects = append(objects, logObj)
	if hadStderr {
		errObj, err := uploadCapture(transport, remoteDir, jobID+".stderr.gz", stderrPath, "stderr", true)
		if err != nil {
			return fmt.Errorf("capture archive failed: %w", err)
		}
		objects = append(objects, errObj)
	}
	manifestBytes, err := manifestDocument(jobID, date, objects)
	if err != nil {
		return err
	}
	manifestPath, err := writeTemp("supervisor-manifest-*.json", manifestBytes)
	if err != nil {
		return err
	}
	defer os.Remove(manifestPath)
	manifestObj, err := uploadCapture(transport, remoteDir, "manifest.json", manifestPath, "manifest", false)
	if err != nil {
		return fmt.Errorf("capture archive failed: %w", err)
	}
	meta := &CaptureArchive{
		ArchivedAt:   time.Now().UTC(),
		Date:         date,
		Host:         spec.userHost,
		Root:         spec.root,
		RemoteDir:    remoteDir,
		Files:        append(objects, manifestObj),
		ManifestPath: manifestObj.RemotePath,
		ManifestSHA:  manifestObj.OriginalSHA256,
	}
	if err := verifyStoredArchive(ctx, spec, transport, meta); err != nil {
		return fmt.Errorf("capture archive refused: %w", err)
	}
	if err := saveArchiveMetadata(store, job, meta); err != nil {
		return err
	}
	return removeVerifiedLocals(store, job, meta, false)
}

func saveArchiveMetadata(store *JobStore, job *Job, meta *CaptureArchive) error {
	job.mu.Lock()
	prev := cloneArchive(job.CaptureArchive)
	job.CaptureArchive = cloneArchive(meta)
	job.mu.Unlock()
	if err := store.save(job); err != nil {
		job.mu.Lock()
		job.CaptureArchive = prev
		job.mu.Unlock()
		return fmt.Errorf("archive metadata was not saved: %w", err)
	}
	return nil
}

func removeVerifiedLocals(store *JobStore, job *Job, meta *CaptureArchive, alreadyRemoved bool) error {
	if alreadyRemoved {
		return nil
	}
	logPath := ""
	for _, obj := range meta.Files {
		if obj.Kind == "log" {
			logPath = obj.LocalPath
		}
	}
	if logPath == "" {
		return fmt.Errorf("capture archive refused: manifest has no log")
	}
	if err := jobLogOwned(store, job.ID, logPath); err != nil {
		return fmt.Errorf("capture archive refused: %w", err)
	}

	for _, obj := range meta.Files {
		if obj.Kind != "log" && obj.Kind != "stderr" {
			continue
		}
		if err := verifyLocalOriginal(obj); err != nil {
			return fmt.Errorf("capture archive refused: %w", err)
		}
	}
	for _, obj := range meta.Files {
		if obj.Kind != "log" && obj.Kind != "stderr" {
			continue
		}
		if err := os.Remove(obj.LocalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("capture archive verified but local %s remains: %w", obj.Kind, err)
		}
		if obj.Kind == "log" {
			if err := os.Remove(captureGzipPath(obj.LocalPath)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("capture archive verified but local %s remains: %w", obj.Kind, err)
			}
		}
	}
	job.mu.Lock()
	if job.CaptureArchive != nil {
		job.CaptureArchive.LocalRemoved = true
	}
	job.mu.Unlock()
	if err := store.save(job); err != nil {
		job.mu.Lock()
		if job.CaptureArchive != nil {
			job.CaptureArchive.LocalRemoved = false
		}
		job.mu.Unlock()
		log.Printf("job %s: archive removed local capture but could not record it: %v", job.ID, err)
		return fmt.Errorf("archive metadata was not saved: %w", err)
	}
	return nil
}

func verifyLocalOriginal(obj ArchiveObject) error {
	sum, size, _, err := hashLocalCapture(obj.LocalPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if sum != obj.OriginalSHA256 || size != obj.OriginalSize {
		return fmt.Errorf("local %s differs from the archived original", obj.Kind)
	}
	return nil
}

func restoreJobCapture(ctx context.Context, store *JobStore, job *Job) error {
	job.archiveMu.Lock()
	defer job.archiveMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	job.mu.RLock()
	if !jobTerminal(job.Status) {
		job.mu.RUnlock()
		return fmt.Errorf("restore refused: job capture is still active")
	}
	meta := cloneArchive(job.CaptureArchive)
	logPath := job.LogPath
	jobID := job.ID
	job.mu.RUnlock()
	if meta == nil || meta.ManifestPath == "" {
		return fmt.Errorf("restore refused: job has no archive metadata")
	}
	target := strings.TrimSpace(os.Getenv("SUPERVISOR_CAPTURE_ARCHIVE_TARGET"))
	spec, err := parseArchiveTarget(target)
	if err != nil {
		return fmt.Errorf("restore refused: %w", err)
	}
	if spec.userHost != meta.Host || spec.root != meta.Root {
		return fmt.Errorf("restore refused: archive target does not match stored metadata")
	}
	if err := jobLogOwned(store, jobID, logPath); err != nil {
		return fmt.Errorf("restore refused: %w", err)
	}
	transport, err := archiveTransportForContext(ctx, spec)
	if err != nil {
		return fmt.Errorf("restore refused: %w", err)
	}
	if err := verifyStoredArchive(ctx, spec, transport, meta); err != nil {
		return fmt.Errorf("restore refused: %w", err)
	}

	type stagedFile struct {
		dest string
		tmp  string
	}
	var plan []stagedFile
	discard := func() {
		for _, f := range plan {
			if f.tmp != "" {
				os.Remove(f.tmp)
			}
		}
	}
	for _, obj := range meta.Files {
		if obj.Kind != "log" && obj.Kind != "stderr" {
			continue
		}
		dest := logPath
		if obj.Kind == "stderr" {
			dest = logPath + ".stderr"
		}
		if filepath.Clean(obj.LocalPath) != dest {
			discard()
			return fmt.Errorf("restore refused: archive path does not match %s", obj.Kind)
		}
		tmp, err := stageRestoreFile(ctx, transport, obj, dest)
		if err != nil {
			discard()
			return fmt.Errorf("restore refused: %w", err)
		}
		plan = append(plan, stagedFile{dest: dest, tmp: tmp})
	}
	if len(plan) == 0 {
		return fmt.Errorf("restore refused: archive metadata has no log")
	}

	if !meta.RestoreIntent {
		meta.RestoreIntent = true
		if err := saveArchiveMetadata(store, job, meta); err != nil {
			discard()
			return fmt.Errorf("restore refused: could not persist restore intent: %w", err)
		}
	}
	for i := range plan {
		if plan[i].tmp == "" {
			continue
		}
		if err := os.Rename(plan[i].tmp, plan[i].dest); err != nil {
			discard()
			return fmt.Errorf("restore failed for %s: %w", plan[i].dest, err)
		}
		plan[i].tmp = ""
	}
	meta.RestoreIntent = false
	meta.LocalRestored = true
	meta.LocalRemoved = false
	if err := saveArchiveMetadata(store, job, meta); err != nil {
		return fmt.Errorf("restore completed but metadata was not saved: %w", err)
	}
	return nil
}

func stageRestoreFile(ctx context.Context, tr archiveTransport, obj ArchiveObject, dest string) (string, error) {
	sum, size, _, err := hashLocalCapture(dest)
	if err == nil {
		if sum == obj.OriginalSHA256 && size == obj.OriginalSize {
			return "", nil
		}
		return "", fmt.Errorf("local capture differs from the archived original")
	}
	if !os.IsNotExist(err) {
		return "", err
	}
	dir := filepath.Dir(dest)
	gzFile, err := os.CreateTemp(dir, ".restore-gz-*")
	if err != nil {
		return "", err
	}
	gzName := gzFile.Name()
	gzFile.Close()
	defer os.Remove(gzName)
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := tr.Get(obj.RemotePath, gzName); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	gotSum, gotSize, err := hashFile(gzName)
	if err != nil {
		return "", err
	}
	if gotSum != obj.CompressedSHA256 || gotSize != obj.CompressedSize {
		return "", fmt.Errorf("compressed checksum mismatch")
	}
	rawFile, err := os.CreateTemp(dir, ".restore-raw-*")
	if err != nil {
		return "", err
	}
	rawName := rawFile.Name()
	rawFile.Close()
	if err := gunzipFile(gzName, rawName); err != nil {
		os.Remove(rawName)
		return "", err
	}
	rawSum, rawSize, err := hashFile(rawName)
	if err != nil {
		os.Remove(rawName)
		return "", err
	}
	if rawSum != obj.OriginalSHA256 || rawSize != obj.OriginalSize {
		os.Remove(rawName)
		return "", fmt.Errorf("original checksum mismatch")
	}
	if err := os.Chmod(rawName, 0600); err != nil {
		os.Remove(rawName)
		return "", err
	}
	return rawName, nil
}

func uploadCapture(tr archiveTransport, remoteDir, remoteName, localPath, kind string, compress bool) (ArchiveObject, error) {
	rawSum, rawSize, err := hashFile(localPath)
	uploadPath := localPath
	compSum, compSize := rawSum, rawSize
	if compress && errors.Is(err, os.ErrNotExist) {

		var compressed bool
		rawSum, rawSize, compressed, err = hashLocalCapture(localPath)
		if err != nil {
			return ArchiveObject{}, err
		}
		if !compressed {
			return ArchiveObject{}, os.ErrNotExist
		}
		uploadPath = captureGzipPath(localPath)
		compSum, compSize, err = hashFile(uploadPath)
		if err != nil {
			return ArchiveObject{}, err
		}
	} else if err != nil {
		return ArchiveObject{}, err
	} else if compress {
		tmp, err := os.CreateTemp("", "supervisor-capture-*.gz")
		if err != nil {
			return ArchiveObject{}, err
		}
		tmpName := tmp.Name()
		tmp.Close()
		defer os.Remove(tmpName)
		if err := gzipFile(localPath, tmpName); err != nil {
			return ArchiveObject{}, err
		}
		compSum, compSize, err = hashFile(tmpName)
		if err != nil {
			return ArchiveObject{}, err
		}
		uploadPath = tmpName
	}
	sha, size, err := tr.Put(remoteDir, remoteName, uploadPath)
	if err != nil {
		return ArchiveObject{}, err
	}
	if sha != compSum || size != compSize {
		return ArchiveObject{}, fmt.Errorf("remote checksum mismatch for %s", remoteName)
	}
	obj := ArchiveObject{
		Kind:             kind,
		RemotePath:       path.Join(remoteDir, remoteName),
		OriginalSHA256:   rawSum,
		OriginalSize:     rawSize,
		CompressedSHA256: compSum,
		CompressedSize:   compSize,
	}
	if kind != "manifest" {
		obj.LocalPath = localPath
	}
	return obj, nil
}

func manifestDocument(jobID, date string, objects []ArchiveObject) ([]byte, error) {
	doc := archiveManifest{JobID: jobID, Date: date}
	for _, obj := range objects {
		if obj.Kind == "manifest" {
			continue
		}
		doc.Files = append(doc.Files, archiveManifestFile{
			Kind:             obj.Kind,
			RemoteName:       path.Base(obj.RemotePath),
			OriginalSHA256:   obj.OriginalSHA256,
			OriginalSize:     obj.OriginalSize,
			CompressedSHA256: obj.CompressedSHA256,
			CompressedSize:   obj.CompressedSize,
		})
	}
	if len(doc.Files) == 0 {
		return nil, fmt.Errorf("capture archive refused: empty manifest")
	}
	return json.MarshalIndent(doc, "", "  ")
}

func verifyStoredArchive(ctx context.Context, spec archiveTarget, tr archiveTransport, meta *CaptureArchive) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if meta == nil || meta.RemoteDir == "" || meta.ManifestPath == "" || meta.ManifestSHA == "" {
		return fmt.Errorf("archive metadata is incomplete")
	}
	if spec.userHost != meta.Host || spec.root != meta.Root {
		return fmt.Errorf("archive target does not match stored metadata")
	}
	if !provenDisjoint(spec.captures, forbiddenCaptureArchive) || !hasPathPrefix(meta.RemoteDir, spec.captures) || !provenDisjoint(meta.RemoteDir, forbiddenCaptureArchive) {
		return fmt.Errorf("archive root overlaps the recovery archive")
	}
	if tr == nil {
		return fmt.Errorf("archive transport unavailable")
	}
	var sawLog bool
	var manifest *ArchiveObject
	for i := range meta.Files {
		obj := meta.Files[i]
		if err := validateRemotePath(obj.RemotePath); err != nil || !hasPathPrefix(obj.RemotePath, meta.RemoteDir) {
			return fmt.Errorf("archive object path is not proven safe")
		}
		sha, size, err := tr.Stat(obj.RemotePath)
		if err != nil {
			return err
		}
		if sha != obj.CompressedSHA256 || size != obj.CompressedSize {
			return fmt.Errorf("remote checksum mismatch for %s", obj.Kind)
		}
		switch obj.Kind {
		case "log":
			sawLog = true
		case "manifest":
			copyObj := obj
			manifest = &copyObj
		}
	}
	if !sawLog || manifest == nil {
		return fmt.Errorf("archive metadata is missing the log or manifest")
	}
	if manifest.RemotePath != meta.ManifestPath || manifest.OriginalSHA256 != meta.ManifestSHA || manifest.CompressedSHA256 != meta.ManifestSHA {
		return fmt.Errorf("manifest checksum does not match archive metadata")
	}
	manifestPath, err := writeTemp("supervisor-manifest-verify-*.json", nil)
	if err != nil {
		return err
	}
	defer os.Remove(manifestPath)
	if err := tr.Get(manifest.RemotePath, manifestPath); err != nil {
		return fmt.Errorf("manifest download failed: %w", err)
	}
	manifestSHA, manifestSize, err := hashFile(manifestPath)
	if err != nil {
		return err
	}
	if manifestSHA != manifest.OriginalSHA256 || manifestSize != manifest.OriginalSize {
		return fmt.Errorf("manifest checksum mismatch")
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	var document archiveManifest
	if err := json.Unmarshal(manifestBytes, &document); err != nil {
		return fmt.Errorf("manifest is invalid: %w", err)
	}
	if document.JobID != path.Base(meta.RemoteDir) || document.Date != path.Base(path.Dir(meta.RemoteDir)) || !manifestMatches(meta.Files, document) {
		return fmt.Errorf("manifest contents do not match archive metadata")
	}
	return nil
}

func manifestMatches(objects []ArchiveObject, manifest archiveManifest) bool {
	files := make(map[string]ArchiveObject, len(objects))
	for _, obj := range objects {
		if obj.Kind == "log" || obj.Kind == "stderr" {
			files[obj.Kind] = obj
		}
	}
	if len(files) != len(manifest.Files) {
		return false
	}
	for _, file := range manifest.Files {
		obj, ok := files[file.Kind]
		if !ok || path.Base(obj.RemotePath) != file.RemoteName || obj.OriginalSHA256 != file.OriginalSHA256 || obj.OriginalSize != file.OriginalSize || obj.CompressedSHA256 != file.CompressedSHA256 || obj.CompressedSize != file.CompressedSize {
			return false
		}
		delete(files, file.Kind)
	}
	return len(files) == 0
}

func cloneArchive(in *CaptureArchive) *CaptureArchive {
	if in == nil {
		return nil
	}
	out := *in
	out.Files = append([]ArchiveObject(nil), in.Files...)
	return &out
}

func jobLogOwned(store *JobStore, id, logPath string) error {
	if store == nil || !jobIDToken(id) {
		return fmt.Errorf("job id is not archive-safe")
	}
	clean := filepath.Clean(logPath)
	rel, err := filepath.Rel(filepath.Clean(store.logsDir), clean)
	if err != nil || rel != id+".log" {
		return fmt.Errorf("refusing to touch a log outside the job record")
	}
	return nil
}

func parseArchiveTarget(raw string) (archiveTarget, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return archiveTarget{}, fmt.Errorf("archive target is empty")
	}
	at := strings.IndexByte(raw, '@')
	colon := strings.LastIndexByte(raw, ':')
	if at <= 0 || colon <= at+1 || colon == len(raw)-1 || strings.Count(raw, ":") != 1 {
		return archiveTarget{}, fmt.Errorf("archive target is invalid")
	}
	user := raw[:at]
	host := raw[at+1 : colon]
	root := raw[colon+1:]
	if !userOK(user) || !hostOK(host) || !absPathOK(root) {
		return archiveTarget{}, fmt.Errorf("archive target is invalid")
	}
	root = path.Clean(root)
	captures := path.Join(root, "supervisor-captures")
	if !provenDisjoint(root, forbiddenCaptureArchive) && hasPathPrefix(root, forbiddenCaptureArchive) {
		return archiveTarget{}, fmt.Errorf("archive target overlaps the recovery archive")
	}
	if !provenDisjoint(captures, forbiddenCaptureArchive) {
		return archiveTarget{}, fmt.Errorf("archive target overlaps the recovery archive")
	}
	if !hasPathPrefix(captures, root) && root != "/" {
		return archiveTarget{}, fmt.Errorf("archive target is invalid")
	}
	return archiveTarget{userHost: user + "@" + host, root: root, captures: captures}, nil
}

func provenDisjoint(a, b string) bool {
	if a == "" || b == "" {
		return true
	}
	a = path.Clean(a)
	b = path.Clean(b)
	if !path.IsAbs(a) || !path.IsAbs(b) || strings.Contains(a, "..") || strings.Contains(b, "..") {
		return false
	}
	if a == b || hasPathPrefix(a, b) || hasPathPrefix(b, a) {
		return false
	}
	return true
}

func hasPathPrefix(p, prefix string) bool {
	p = path.Clean(p)
	prefix = path.Clean(prefix)
	if prefix == "/" {
		return p != "/"
	}
	return p == prefix || strings.HasPrefix(p, prefix+"/")
}

func absPathOK(p string) bool {
	if !strings.HasPrefix(p, "/") || strings.Contains(p, "..") || strings.Contains(p, "//") || strings.ContainsAny(p, " \t\r\n") {
		return false
	}
	return path.Clean(p) == p && remotePathToken(p)
}

func remotePathToken(p string) bool {
	if len(p) == 0 || len(p) > 512 || !strings.HasPrefix(p, "/") {
		return false
	}
	for _, seg := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
		for _, r := range seg {
			if !tokenRune(r, true) {
				return false
			}
		}
	}
	return true
}

func validateRemotePath(p string) error {
	if !remotePathToken(p) || path.Clean(p) != p {
		return fmt.Errorf("remote path rejected")
	}
	return nil
}

func jobIDToken(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, r := range id {
		if !tokenRune(r, false) {
			return false
		}
	}
	return true
}

func userOK(s string) bool {
	if len(s) == 0 || len(s) > 32 {
		return false
	}
	for i, r := range s {
		if i == 0 && !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_') {
			return false
		}
		if !tokenRune(r, true) {
			return false
		}
	}
	return true
}

func hostOK(s string) bool {
	if len(s) == 0 || len(s) > 253 || strings.Contains(s, "..") {
		return false
	}
	for _, r := range s {
		if r != '.' && r != '-' && !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

func tokenRune(r rune, dot bool) bool {
	if r == '_' || r == '-' || (dot && r == '.') {
		return true
	}
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
}

func validateName(name string) error {
	if name == "" || len(name) > 180 || strings.Contains(name, "/") || strings.Contains(name, "..") {
		return fmt.Errorf("remote name rejected")
	}
	for _, r := range name {
		if !tokenRune(r, true) {
			return fmt.Errorf("remote name rejected")
		}
	}
	return nil
}

func (s *sshArchive) allow(p string) error {
	if err := validateRemotePath(p); err != nil {
		return err
	}
	if p != s.spec.captures && !hasPathPrefix(p, s.spec.captures) {
		return fmt.Errorf("remote path escapes supervisor-captures")
	}
	if !provenDisjoint(s.spec.captures, forbiddenCaptureArchive) || !provenDisjoint(p, forbiddenCaptureArchive) {
		return fmt.Errorf("archive path overlaps the recovery archive")
	}
	return nil
}

func (s *sshArchive) Put(remoteDir, finalName, localPath string) (string, int64, error) {
	if err := validateName(finalName); err != nil {
		return "", 0, err
	}
	remoteFinal := path.Join(remoteDir, finalName)
	if err := s.allow(remoteDir); err != nil {
		return "", 0, err
	}
	if err := s.allow(remoteFinal); err != nil {
		return "", 0, err
	}
	if _, err := s.remote(s.requestContext(), "mkdir", remoteDir); err != nil {
		return "", 0, err
	}
	wantSum, wantSize, err := hashFile(localPath)
	if err != nil {
		return "", 0, err
	}
	if sum, size, err := s.Stat(remoteFinal); err == nil {
		if sum == wantSum && size == wantSize {
			return sum, size, nil
		}
		return "", 0, fmt.Errorf("remote object does not match local bytes")
	} else if !errors.Is(err, errArchiveNotFound) {
		return "", 0, err
	}
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return "", 0, err
	}
	partialName := ".partial-" + hex.EncodeToString(nonce) + "-" + finalName
	if err := validateName(partialName); err != nil {
		return "", 0, err
	}
	partial := path.Join(remoteDir, partialName)
	if err := s.allow(partial); err != nil {
		return "", 0, err
	}
	if err := s.scpContext(s.requestContext(), localPath, partial, true); err != nil {
		return "", 0, err
	}
	sum, size, err := s.Stat(partial)
	if err != nil || sum != wantSum || size != wantSize {
		_, _ = s.remote(s.requestContext(), "rmfile", partial)
		return "", 0, fmt.Errorf("uploaded object failed checksum")
	}
	if _, err := s.remote(s.requestContext(), "rename", partial, remoteFinal); err != nil {
		_, _ = s.remote(s.requestContext(), "rmfile", partial)
		return "", 0, err
	}
	sum, size, err = s.Stat(remoteFinal)
	if err != nil || sum != wantSum || size != wantSize {
		return "", 0, fmt.Errorf("renamed object failed checksum")
	}
	return sum, size, nil
}

func (s *sshArchive) Stat(remotePath string) (string, int64, error) {
	if err := s.allow(remotePath); err != nil {
		return "", 0, err
	}
	out, err := s.remote(s.requestContext(), "sum", remotePath)
	if err != nil {
		return "", 0, err
	}
	text := strings.TrimSpace(out)
	if text == "MISSING" {
		return "", 0, errArchiveNotFound
	}
	lines := strings.Split(text, "\n")
	if len(lines) < 2 || len(strings.TrimSpace(lines[0])) != 64 {
		return "", 0, fmt.Errorf("unexpected checksum response")
	}
	size, err := strconv.ParseInt(strings.TrimSpace(lines[1]), 10, 64)
	if err != nil || size < 0 {
		return "", 0, fmt.Errorf("unexpected checksum response")
	}
	return strings.TrimSpace(lines[0]), size, nil
}

func (s *sshArchive) Get(remotePath, localPath string) error {
	if err := s.allow(remotePath); err != nil {
		return err
	}
	return s.scpContext(s.requestContext(), localPath, remotePath, false)
}

func (s *sshArchive) ListDates(capturesRoot string) ([]string, error) {
	if path.Clean(capturesRoot) != s.spec.captures || path.Base(capturesRoot) != "supervisor-captures" {
		return nil, fmt.Errorf("refusing to list outside supervisor-captures")
	}
	out, err := s.remote(s.requestContext(), "listdates", capturesRoot)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if dateNameOK(line) {
			names = append(names, line)
		}
	}
	return names, nil
}

func (s *sshArchive) RemoveDate(dateDir string) error {
	if path.Base(path.Dir(dateDir)) != "supervisor-captures" || !dateNameOK(path.Base(dateDir)) {
		return fmt.Errorf("refusing to remove outside a capture date directory")
	}
	if err := s.allow(dateDir); err != nil {
		return err
	}
	_, err := s.remote(s.requestContext(), "rmdatedir", dateDir)
	return err
}

func (s *sshArchive) remote(ctx context.Context, op string, args ...string) (string, error) {
	for _, arg := range args {
		if err := validateRemotePath(arg); err != nil {
			return "", err
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	remoteEnv := ""
	if forbiddenCaptureArchive != "" && validateRemotePath(forbiddenCaptureArchive) == nil {
		remoteEnv = "SUPERVISOR_CAPTURE_FORBIDDEN=" + forbiddenCaptureArchive
	}
	argv := []string{
		"-o", "BatchMode=yes",
		"-o", "NumberOfPasswordPrompts=0",
		"-o", "ConnectTimeout=20",
		"-o", "StrictHostKeyChecking=yes",
		s.spec.userHost,
	}
	if remoteEnv != "" {
		argv = append(argv, remoteEnv)
	}
	argv = append(argv, "bash", "-s", "--", op)
	argv = append(argv, args...)
	cmd := exec.CommandContext(ctx, "ssh", argv...)
	cmd.Stdin = strings.NewReader(remoteArchiveScript)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		log.Printf("capture archive ssh %s failed: %v", op, err)
		return "", fmt.Errorf("ssh %s failed", op)
	}
	return stdout.String(), nil
}

func (s *sshArchive) scp(localPath, remotePath string, upload bool) error {
	return s.scpContext(context.Background(), localPath, remotePath, upload)
}

func (s *sshArchive) scpContext(parent context.Context, localPath, remotePath string, upload bool) error {
	spec := s.spec.userHost + ":" + remotePath
	argv := []string{
		"-o", "BatchMode=yes",
		"-o", "NumberOfPasswordPrompts=0",
		"-o", "ConnectTimeout=20",
		"-o", "StrictHostKeyChecking=yes",
	}
	if upload {
		argv = append(argv, localPath, spec)
	} else {
		argv = append(argv, spec, localPath)
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "scp", argv...)
	if err := cmd.Run(); err != nil {
		log.Printf("capture archive scp failed: %v", err)
		return fmt.Errorf("scp failed")
	}
	return nil
}

func archiveCaptureWithRetry(store *JobStore, job *Job) {
	for attempt := range 8 {
		if err := archiveJobCapture(context.Background(), store, job); err == nil {
			return
		} else {
			log.Printf("job %s: capture archive attempt %d failed; local capture retained: %v", job.ID, attempt+1, err)
		}
		if attempt == 7 {
			return
		}
		delay := time.Second << attempt
		if delay > 5*time.Minute {
			delay = 5 * time.Minute
		}
		archiveRetryWait(delay)
	}
}
func startCaptureRetentionWorker() {
	go retryAcknowledgedCaptures(globalStore)
	go compressFinishedCaptures(globalStore)
	go func() {
		runCaptureRetentionSafely()
		ticker := time.NewTicker(time.Hour)
		for range ticker.C {
			runCaptureRetentionSafely()
			retryAcknowledgedCaptures(globalStore)
		}
	}()
}

func runCaptureRetentionSafely() {
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("capture retention pass failed: %v", rec)
		}
	}()
	runCaptureRetention()
}

func runCaptureRetention() {
	target := strings.TrimSpace(os.Getenv("SUPERVISOR_CAPTURE_ARCHIVE_TARGET"))
	if target == "" {
		return
	}
	spec, err := parseArchiveTarget(target)
	if err != nil {
		log.Printf("capture retention: %v", err)
		return
	}
	transport, err := archiveTransportForContext(context.Background(), spec)
	if err != nil {
		log.Printf("capture retention: %v", err)
		return
	}
	names, err := transport.ListDates(spec.captures)
	if err != nil {
		log.Printf("capture retention: list failed: %v", err)
		return
	}
	days := captureRetentionDays()
	now := time.Now().UTC()
	for _, name := range names {
		if !captureDateExpired(name, now, days) {
			continue
		}
		dir := path.Join(spec.captures, name)
		if path.Clean(dir) != dir || path.Base(dir) != name || !hasPathPrefix(dir, spec.captures) || !provenDisjoint(dir, forbiddenCaptureArchive) {
			log.Printf("capture retention: skipped unsafe date name")
			continue
		}
		if err := transport.RemoveDate(dir); err != nil {
			log.Printf("capture retention: remove %s failed: %v", name, err)
		}
	}
}

func captureRetentionDays() int {
	raw := strings.TrimSpace(os.Getenv("SUPERVISOR_CAPTURE_ARCHIVE_RETENTION_DAYS"))
	if raw == "" {
		return 30
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 365 {
		log.Printf("capture retention: SUPERVISOR_CAPTURE_ARCHIVE_RETENTION_DAYS %q is outside 1..365; using 30", raw)
		return 30
	}
	return n
}

func dateNameOK(name string) bool {
	if len(name) != 10 {
		return false
	}
	day, err := time.Parse("2006-01-02", name)
	return err == nil && day.Format("2006-01-02") == name
}

func captureDateExpired(name string, now time.Time, days int) bool {
	if !dateNameOK(name) || days < 1 {
		return false
	}
	day, err := time.Parse("2006-01-02", name)
	if err != nil {
		return false
	}
	cutoff := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -days)
	return day.Before(cutoff)
}

func hashFile(p string) (string, int64, error) {
	f, err := openRegular(p)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

func openRegular(p string) (*os.File, error) {
	f, err := openNoFollow(p)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("not a regular file")
	}
	return f, nil
}

func gzipFile(src, dst string) error {
	in, err := openRegular(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	gz := gzip.NewWriter(out)
	gz.Header = gzip.Header{Name: "", ModTime: time.Time{}, OS: 255}
	if _, err := io.Copy(gz, in); err != nil {
		gz.Close()
		out.Close()
		return err
	}
	if err := gz.Close(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func gunzipFile(src, dst string) error {
	in, err := openRegular(src)
	if err != nil {
		return err
	}
	defer in.Close()
	gz, err := gzip.NewReader(in)
	if err != nil {
		return err
	}
	defer gz.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, gz); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func copyRegular(src, dst string) error {
	in, err := openRegular(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func writeTemp(pattern string, body []byte) (string, error) {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", err
	}
	name := f.Name()
	if _, err := f.Write(body); err != nil {
		f.Close()
		os.Remove(name)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(name)
		return "", err
	}
	return name, nil
}
