package main

import (
	"fmt"
	"sync"
)

var continuationMu sync.Mutex

const continuationChainLimit = 200

func continueFinishedJob(jobID, mode, text string, source *PromptSource) (map[string]any, error) {
	continuationMu.Lock()
	defer continuationMu.Unlock()

	tailID := jobID
	for hops := 0; ; hops++ {
		if hops > continuationChainLimit {
			return nil, fmt.Errorf("job %s continuation chain does not terminate after %d hops; refusing to follow corrupted linkage", jobID, continuationChainLimit)
		}
		tail := globalStore.get(tailID)
		if tail == nil {
			return nil, fmt.Errorf("job %q not found", tailID)
		}
		tail.mu.RLock()
		status := tail.Status
		pid := tail.PID
		next := tail.ContinuedByJobID
		tail.mu.RUnlock()
		if status == "running" && !isProcessAlive(pid) {
			status = "error"
		}
		if status == "running" {

			return steerJob(tailID, mode, text, source)
		}
		if next == "" {
			break
		}
		tailID = next
	}

	tail := globalStore.get(tailID)
	tail.mu.RLock()
	if tail.SessionID == "" {
		status := tail.Status
		tail.mu.RUnlock()
		return nil, fmt.Errorf("job %s is %s and has no resumable OMP session (it ended before its session was created); start a fresh run instead", tailID, status)
	}
	spec := jobSpec{
		Machine:            tail.Machine,
		Prompt:             text,
		Source:             source,
		RawModel:           tail.ModelRequested,
		Thinking:           tail.ThinkingRequested,
		CWD:                tail.CWD,
		MaxTime:            tail.MaxTime,
		Title:              tail.Title,
		SessionID:          tail.SessionID,
		Yolo:               tail.Yolo,
		Steerable:          tail.Steerable,
		ParentAgentID:      tail.ParentAgentID,
		NotifyOnCompletion: tail.NotifyOnCompletion,
	}
	tail.mu.RUnlock()

	child, err := startJob(spec)
	if err != nil {
		return nil, fmt.Errorf("continuing job %s failed to launch: %v", tailID, err)
	}

	child.mu.Lock()
	child.ContinuedFromJobID = tailID
	child.mu.Unlock()
	globalStore.save(child)

	tail.mu.Lock()
	tail.ContinuedByJobID = child.ID
	tail.mu.Unlock()
	globalStore.save(tail)

	resp := map[string]any{
		"job_id":           child.ID,
		"id":               child.ID,
		"pid":              child.PID,
		"continued":        true,
		"continued_job_id": tailID,
		"mode":             mode,
		"status":           child.Status,
		"session_id":       child.SessionID,
		"log_path":         child.LogPath,
	}
	if tailID != jobID {

		resp["requested_job_id"] = jobID
	}
	return resp, nil
}
