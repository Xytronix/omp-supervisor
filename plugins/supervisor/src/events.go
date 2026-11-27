package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"

	"github.com/mark3labs/mcp-go/mcp"
)

func newRPCLogScanner(reader io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), rpcFrameMax)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if end := bytes.IndexByte(data, '\n'); end >= 0 {
			return end + 1, data[:end+1], nil
		}
		return 0, nil, nil
	})
	return scanner
}

type jobEvent struct {
	Cursor int64           `json:"cursor"`
	Event  json.RawMessage `json:"event"`
}

type jobEventPage struct {
	FromCursor int64      `json:"from_cursor"`
	NextCursor int64      `json:"next_cursor"`
	Reset      bool       `json:"reset"`
	HasMore    bool       `json:"has_more"`
	Events     []jobEvent `json:"events"`
}

func safeChildEvent(line []byte) bool {
	var envelope struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(line, &envelope) != nil {
		return false
	}
	switch envelope.Type {
	case "agent_start", "agent_end", "turn_start", "turn_end",
		"message_start", "message_update", "message_end",
		"tool_execution_start", "tool_execution_update", "tool_execution_end",
		"model_changed", "thinking_level_changed",
		"retry_fallback_applied", "retry_fallback_succeeded":
		return true
	default:
		return false
	}
}

func readJobEvents(ctx context.Context, path string, after int64, limit int) (jobEventPage, error) {
	page := jobEventPage{FromCursor: after, NextCursor: after, Events: make([]jobEvent, 0)}

	capture, err := openCapture(path)
	if err != nil {
		return page, fmt.Errorf("RPC log unavailable: %w", err)
	}
	defer capture.Close()
	size := capture.Size()
	var reader io.Reader

	if after > size {
		page.Reset = true
	} else if after > 0 {
		reader, err = capture.ReaderFrom(after - 1)
		if err != nil {
			return page, fmt.Errorf("RPC cursor boundary: %w", err)
		}
		var previous [1]byte
		if _, err := io.ReadFull(reader, previous[:]); err != nil {
			return page, fmt.Errorf("RPC cursor boundary: %w", err)
		}
		page.Reset = previous[0] != '\n'
	}
	if page.Reset {
		page.FromCursor, page.NextCursor = 0, 0
		reader = nil
	}
	if page.NextCursor == size {

		return page, ctx.Err()
	}
	if reader == nil {
		if reader, err = capture.ReaderFrom(page.NextCursor); err != nil {
			return page, fmt.Errorf("RPC log read: %w", err)
		}
	}

	scanner := newRPCLogScanner(io.LimitReader(reader, size-page.NextCursor))
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return page, err
		}

		if len(page.Events) >= limit || page.NextCursor-page.FromCursor >= rpcFrameMax {
			page.HasMore = true
			break
		}
		line := scanner.Bytes()
		page.NextCursor += int64(len(line))
		if safeChildEvent(line) {
			page.Events = append(page.Events, jobEvent{
				Cursor: page.NextCursor,
				Event:  append(json.RawMessage(nil), bytes.TrimSpace(line)...),
			})
		}
	}
	if err := scanner.Err(); err != nil {
		return page, fmt.Errorf("RPC log read: %w", err)
	}
	return page, ctx.Err()
}

func eventInteger(args map[string]any, key string, def int64) (int64, error) {
	raw, ok := args[key]
	if !ok {
		return def, nil
	}
	var n float64
	switch value := raw.(type) {
	case float64:
		n = value
	case int:
		n = float64(value)
	case int64:
		n = float64(value)
	case json.Number:
		var err error
		n, err = value.Float64()
		if err != nil {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
	default:
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	if math.IsNaN(n) || math.IsInf(n, 0) || math.Trunc(n) != n || n < 0 || n > 9007199254740991 {
		return 0, fmt.Errorf("%s must be a nonnegative safe integer", key)
	}
	return int64(n), nil
}

func handleEvents(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	id := getStringArg(args, "job_id", "")
	if id == "" {
		return mcp.NewToolResultError("job_id is required"), nil
	}
	store := globalStore
	job := store.get(id)
	if job == nil {
		return mcp.NewToolResultError(fmt.Sprintf("job %q not found", id)), nil
	}
	after, err := eventInteger(args, "after", 0)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	limit, err := eventInteger(args, "limit", 100)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if limit < 1 {
		limit = 1
	} else if limit > 500 {
		limit = 500
	}
	store.ensureRecoveredMetadata(job)
	store.reconcileRecovered(job)
	job.mu.RLock()
	path, rpc := job.LogPath, job.Steerable

	streamClosed := job.captureDone == nil
	if job.captureDone != nil {
		select {
		case <-job.captureDone:
			streamClosed = true
		default:
		}
	}
	out := map[string]any{
		"job_id": job.ID, "status": job.Status, "stream_closed": streamClosed,
	}
	job.addExecutionMetadataLocked(out)
	captureError := job.CaptureError
	job.mu.RUnlock()
	if !rpc || path == "" {
		return mcp.NewToolResultError("job has no RPC event log"), nil
	}
	page, err := readJobEvents(ctx, path, after, int(limit))
	if err != nil && (ctx.Err() != nil || !streamClosed || captureError == "") {
		return mcp.NewToolResultError(err.Error()), nil
	}

	out["from_cursor"] = page.FromCursor
	out["next_cursor"] = page.NextCursor
	out["reset"] = page.Reset
	out["has_more"] = page.HasMore
	out["events"] = page.Events
	data, err := json.Marshal(out)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}
