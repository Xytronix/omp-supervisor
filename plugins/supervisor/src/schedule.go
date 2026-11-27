package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

type Schedule struct {
	ID                 string        `json:"id"`
	TargetJobID        string        `json:"target_job_id"`
	Prompt             string        `json:"prompt"`
	Source             *PromptSource `json:"source,omitempty"`
	Model              string        `json:"model"`
	Thinking           string        `json:"thinking"`
	CWD                string        `json:"cwd"`
	MaxTime            string        `json:"max_time"`
	Title              string        `json:"title"`
	Yolo               bool          `json:"yolo"`
	Steerable          bool          `json:"steerable"`
	Resume             bool          `json:"resume"`
	ParentAgentID      string        `json:"parent_agent_id,omitempty"`
	NotifyOnCompletion bool          `json:"notify_on_completion,omitempty"`
	Status             string        `json:"status"`
	ChildJobID         string        `json:"child_job_id"`
	Error              string        `json:"error"`
	CreatedAt          time.Time     `json:"created_at"`
	FiredAt            time.Time     `json:"fired_at"`
}

type ScheduleStore struct {
	mu    sync.Mutex
	dir   string
	items map[string]*Schedule
}

var globalSchedules *ScheduleStore

func newScheduleStore(baseDir string) (*ScheduleStore, error) {
	dir := filepath.Join(baseDir, "schedules")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create schedules dir: %w", err)
	}
	s := &ScheduleStore{dir: dir, items: make(map[string]*Schedule)}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		var sch Schedule
		if err := json.Unmarshal(data, &sch); err != nil || sch.ID == "" {
			continue
		}
		s.items[sch.ID] = &sch
	}
	log.Printf("Loaded %d schedules from %s", len(s.items), dir)
	return s, nil
}

func generateScheduleID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("sch_%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("sch_%s_%s", time.Now().Format("20060102_150405"), hex.EncodeToString(b))
}

func (s *ScheduleStore) saveLocked(sch *Schedule) {
	data, err := json.MarshalIndent(sch, "", "  ")
	if err != nil {
		log.Printf("Warning: failed to marshal schedule %s: %v", sch.ID, err)
		return
	}
	path := filepath.Join(s.dir, sch.ID+".json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		log.Printf("Warning: failed to save schedule %s: %v", path, err)
	}
}

func (s *ScheduleStore) add(sch *Schedule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[sch.ID] = sch
	s.saveLocked(sch)
}

func (s *ScheduleStore) get(id string) *Schedule {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sch, ok := s.items[id]; ok {
		copied := *sch
		return &copied
	}
	return nil
}

func (s *ScheduleStore) pendingFor(jobID string) []*Schedule {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Schedule
	for _, sch := range s.items {
		if sch.TargetJobID == jobID && sch.Status == "pending" {
			copied := *sch
			out = append(out, &copied)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (s *ScheduleStore) claimPending(jobID string) []*Schedule {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Schedule
	for _, sch := range s.items {
		if sch.TargetJobID == jobID && sch.Status == "pending" {
			sch.Status = "starting"
			s.saveLocked(sch)
			out = append(out, sch)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func (s *ScheduleStore) cancel(id string) (*Schedule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sch, ok := s.items[id]
	if !ok {
		return nil, fmt.Errorf("schedule %q not found", id)
	}
	if sch.Status != "pending" {
		return sch, fmt.Errorf("schedule %s is already %s", id, sch.Status)
	}
	sch.Status = "canceled"
	s.saveLocked(sch)
	copied := *sch
	return &copied, nil
}

func (s *ScheduleStore) markFired(sch *Schedule, childJobID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.items[sch.ID]
	if !ok {
		stored = sch
		s.items[sch.ID] = stored
	}
	stored.FiredAt = time.Now()
	if err != nil {
		stored.Status = "failed"
		stored.Error = err.Error()
	} else {
		stored.Status = "started"
		stored.ChildJobID = childJobID
	}
	s.saveLocked(stored)
}

func (s *ScheduleStore) onJobTerminal(jobID string) {
	for _, sch := range s.claimPending(jobID) {
		child, err := startJob(sch.spec())
		if err != nil {
			log.Printf("schedule %s failed to start after job %s: %v", sch.ID, jobID, err)
			s.markFired(sch, "", err)
			continue
		}
		log.Printf("schedule %s started job %s after job %s finished", sch.ID, child.ID, jobID)
		s.markFired(sch, child.ID, nil)
	}
}

func (s *ScheduleStore) reconcile() {
	s.mu.Lock()
	targets := make(map[string]bool)
	for _, sch := range s.items {
		if sch.Status == "pending" {
			targets[sch.TargetJobID] = true
		}
	}
	s.mu.Unlock()

	for jobID := range targets {
		target := globalStore.get(jobID)
		if target == nil {
			for _, sch := range s.claimPending(jobID) {
				s.markFired(sch, "", fmt.Errorf("target job %s no longer exists", jobID))
			}
			continue
		}
		target.mu.RLock()
		status := target.Status
		pid := target.PID
		target.mu.RUnlock()
		if status == "running" && isProcessAlive(pid) {
			continue
		}
		s.onJobTerminal(jobID)
	}
}

func (sch *Schedule) spec() jobSpec {

	model := sch.Model
	machine := ""
	if target := globalStore.get(sch.TargetJobID); target != nil {
		target.mu.RLock()
		machine = target.Machine
		target.mu.RUnlock()
	}
	return jobSpec{
		Machine:            machine,
		Prompt:             sch.Prompt,
		Source:             sch.Source,
		RawModel:           model,
		Thinking:           sch.Thinking,
		CWD:                sch.CWD,
		MaxTime:            sch.MaxTime,
		Title:              sch.Title,
		SessionID:          sch.sessionToResume(),
		Yolo:               sch.Yolo,
		Steerable:          sch.Steerable,
		ParentAgentID:      sch.ParentAgentID,
		NotifyOnCompletion: sch.NotifyOnCompletion,
	}
}

func (sch *Schedule) sessionToResume() string {
	if !sch.Resume {
		return ""
	}
	target := globalStore.get(sch.TargetJobID)
	if target == nil {
		return ""
	}
	target.refreshSessionMetadata()
	target.mu.RLock()
	defer target.mu.RUnlock()
	return target.SessionID
}

func handleSchedule(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	source, err := promptSourceArg(args)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	targetID := getJobID(args)
	if targetID == "" {
		return mcp.NewToolResultError("job_id is required (the job to wait for)"), nil
	}
	prompt := getStringArg(args, "prompt", "")
	if prompt == "" {
		prompt = getStringArg(args, "text", "")
	}
	if prompt == "" {
		return mcp.NewToolResultError("prompt is required"), nil
	}

	target := globalStore.get(targetID)
	if target == nil {
		return mcp.NewToolResultError(fmt.Sprintf("job %q not found", targetID)), nil
	}
	parentAgentID := getStringArg(args, "parent_agent_id", "")
	notifyOnCompletion := getBoolArg(args, "notify_on_completion", false)
	if err := validateCompletionNotification(notifyOnCompletion, parentAgentID); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	target.mu.RLock()
	targetStatus := target.Status
	targetPID := target.PID
	targetCWD := target.CWD

	targetModel := target.ModelRequested
	targetThinking := target.ThinkingRequested
	target.mu.RUnlock()

	cwd := getStringArg(args, "cwd", targetCWD)
	model := getStringArg(args, "model", "")
	thinking := getStringArg(args, "thinking", "")
	if model == "" {
		model = targetModel
		if thinking == "" {
			thinking = targetThinking
		}
	}
	if model == "" {
		model = defaultModelSelector
	}
	title := getStringArg(args, "title", "")
	if title == "" {
		title = extractTitle(prompt)
	}

	sch := &Schedule{
		ID:                 generateScheduleID(),
		TargetJobID:        targetID,
		Prompt:             prompt,
		Source:             source,
		Model:              model,
		Thinking:           thinking,
		CWD:                cwd,
		MaxTime:            getStringArg(args, "max_time", ""),
		Title:              title,
		Yolo:               getBoolArg(args, "yolo", false),
		Steerable:          source != nil || getBoolArg(args, "steerable", false),
		Resume:             getBoolArg(args, "resume", true),
		ParentAgentID:      parentAgentID,
		NotifyOnCompletion: notifyOnCompletion,
		Status:             "pending",
		CreatedAt:          time.Now(),
	}
	globalSchedules.add(sch)

	if targetStatus != "running" || !isProcessAlive(targetPID) {
		globalSchedules.onJobTerminal(targetID)
	}

	stored := globalSchedules.get(sch.ID)
	if stored == nil {
		stored = sch
	}

	preview, trunc := headCut(stored.Prompt, promptPreviewMaxChars)
	resp := map[string]any{
		"schedule_id":    stored.ID,
		"target_job_id":  targetID,
		"target_status":  targetStatus,
		"status":         stored.Status,
		"prompt_preview": preview,
		"prompt_chars":   len(stored.Prompt),
		"model":          stored.Model,
		"resume":         stored.Resume,
		"steerable":      stored.Steerable,
		"title":          stored.Title,
	}
	if trunc {
		resp["truncated"] = true
	}
	if stored.Thinking != "" {
		resp["thinking"] = stored.Thinking
	}
	if stored.ChildJobID != "" {
		resp["child_job_id"] = stored.ChildJobID
	}
	if stored.Error != "" {
		resp["error"] = stored.Error
	}

	jsonBytes, err := json.Marshal(resp)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
	}
	return mcp.NewToolResultText(string(jsonBytes)), nil
}
