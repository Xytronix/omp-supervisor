package main

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

type jobSummary struct {
	JobID              string    `json:"job_id"`
	Status             string    `json:"status"`
	Machine            string    `json:"machine,omitempty"`
	Model              string    `json:"model,omitempty"`
	ModelRequested     string    `json:"model_requested,omitempty"`
	Fallback           bool      `json:"fallback,omitempty"`
	ParentSessionID    string    `json:"parent_session_id,omitempty"`
	RootSessionID      string    `json:"root_session_id,omitempty"`
	SessionID          string    `json:"session_id,omitempty"`
	ContinuedFromJobID string    `json:"continued_from_job_id,omitempty"`
	ContinuedByJobID   string    `json:"continued_by_job_id,omitempty"`
	StartTime          time.Time `json:"start_time"`
}

type retainedJobsPage struct {
	Jobs       []jobSummary `json:"jobs"`
	Count      int          `json:"count"`
	Total      int          `json:"total"`
	Offset     int64        `json:"offset"`
	NextOffset int          `json:"next_offset,omitempty"`
}

func (s *JobStore) retainedJobs(limit int, offset int64) retainedJobsPage {
	s.mu.RLock()
	total := len(s.jobs)
	if offset >= int64(total) {
		s.mu.RUnlock()
		return retainedJobsPage{Jobs: []jobSummary{}, Total: total, Offset: offset}
	}
	localMachine := localMachineName()
	rows := make([]jobSummary, 0, total)
	for _, job := range s.jobs {
		job.mu.RLock()
		row := jobSummary{
			JobID: job.ID, Status: job.Status, Machine: job.Machine,
			Model: job.ModelResolved, ModelRequested: job.ModelRequested, Fallback: job.Fallback,
			ParentSessionID: job.ParentSessionID,
			RootSessionID:   job.RootSessionID, StartTime: job.StartTime,
			SessionID:          job.SessionID,
			ContinuedFromJobID: job.ContinuedFromJobID,
			ContinuedByJobID:   job.ContinuedByJobID,
		}
		job.mu.RUnlock()
		if row.Machine == "" {
			row.Machine = localMachine
		}
		rows = append(rows, row)
	}
	s.mu.RUnlock()

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].StartTime.Equal(rows[j].StartTime) {
			return rows[i].JobID < rows[j].JobID
		}
		return rows[i].StartTime.After(rows[j].StartTime)
	})
	page := retainedJobsPage{Total: len(rows), Offset: offset}
	start := int(offset)
	end := start + min(limit, len(rows)-start)
	page.Jobs = rows[start:end]
	page.Count = len(page.Jobs)
	if end < len(rows) {
		page.NextOffset = end
	}
	return page
}

func handleJobs(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	limit, err := eventInteger(args, "limit", 50)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if limit < 1 {
		limit = 1
	} else if limit > 200 {
		limit = 200
	}
	offset, err := eventInteger(args, "offset", 0)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	data, err := json.Marshal(globalStore.retainedJobs(int(limit), offset))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}
