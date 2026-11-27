package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

const (
	maxTimeLimit    = 24 * time.Hour
	maxTimeMinimum  = time.Second
	maxTimeCeiling  = "24h"
	remoteMaxTime   = "6h"
	deadlineGrace   = 3 * time.Second
	deadlineExpired = 124
)

var maxTimePattern = regexp.MustCompile(`^(\d+)(ms|s|m|h)$`)

func parseMaxTime(value string) (time.Duration, error) {
	d, err := parseDuration(value)
	if err != nil {
		return 0, err
	}
	return d, checkMaxTime(d)
}

func parseDuration(value string) (time.Duration, error) {
	match := maxTimePattern.FindStringSubmatch(value)
	if match == nil {
		return 0, fmt.Errorf("invalid duration %q: use an integer with ms, s, m, or h (e.g. 600s, 10m, 1h)", value)
	}
	n, err := strconv.ParseInt(match[1], 10, 64)
	unit := map[string]time.Duration{"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour}[match[2]]
	if err != nil || n > int64(maxTimeLimit/unit) {
		return 0, fmt.Errorf("max_time must be 1 second to 24 hours")
	}
	return time.Duration(n) * unit, nil
}

func checkMaxTime(d time.Duration) error {
	if d < maxTimeMinimum || d > maxTimeLimit {
		return fmt.Errorf("max_time must be 1 second to 24 hours")
	}
	return nil
}

func formatMaxTime(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	default:
		return fmt.Sprintf("%dms", d/time.Millisecond)
	}
}

func (s *JobStore) armDeadlineLocked(j *Job) {
	if j.deadlineTimer != nil {
		j.deadlineTimer.Stop()
		j.deadlineTimer = nil
	}
	if j.Deadline.IsZero() || j.Status != "running" {
		return
	}
	j.deadlineTimer = time.AfterFunc(time.Until(j.Deadline), func() { s.expireDeadline(j) })
}

func (s *JobStore) expireDeadline(j *Job) {
	j.mu.Lock()
	if j.Status != "running" || j.Deadline.IsZero() || time.Now().Before(j.Deadline) {
		j.mu.Unlock()
		return
	}
	remote := j.Machine != "" && j.Machine != localMachineName()
	message := "Agent deadline exceeded"
	if remote {
		message = "Remote agent deadline exceeded"
	}
	j.Status = "error"
	j.ExitCode = deadlineExpired
	if j.EndTime.IsZero() {
		j.EndTime = time.Now()
	}
	if j.Steerable && j.Report == "" {
		j.Report = message
	}
	j.deadlineMessage = message
	pid, child, cancelFn, logPath, steerable := j.PID, j.rpc, j.cancelFn, j.LogPath, j.Steerable
	monitored := j.monitorDone != nil
	j.mu.Unlock()
	log.Printf("job %s: %s", j.ID, message)
	s.save(j)

	if child != nil {
		_, _ = child.command(map[string]any{"type": "abort"}, deadlineGrace, false)
		child.close()
	}
	if pid > 0 {
		terminateProcessGroup(pid)
		exited := false
		for range int(deadlineGrace / (100 * time.Millisecond)) {
			if !isProcessAlive(pid) {
				exited = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !exited {
			killProcessGroup(pid)
		}
	}
	if cancelFn != nil {
		cancelFn()
	}
	if monitored {
		return
	}
	if logPath != "" {
		path := logPath
		if steerable {
			path += ".stderr"
		}
		if f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600); err == nil {
			fmt.Fprintln(f, message)
			f.Close()
		}
	}
	j.refreshSessionMetadata()
	s.save(j)
	notifyCompletionWebhooks(j)
	if globalSchedules != nil {
		globalSchedules.onJobTerminal(j.ID)
	}
}

func (s *JobStore) extendDeadline(id, maxTime, add string) (*Job, error) {
	if (maxTime == "") == (add == "") {
		return nil, fmt.Errorf("pass exactly one of max_time or add")
	}
	j := s.get(id)
	if j == nil {
		return nil, fmt.Errorf("job %q not found", id)
	}
	if err := s.applyExtension(j, maxTime, add); err != nil {
		return nil, err
	}
	if err := s.save(j); err != nil {
		return nil, err
	}
	return j, nil
}

func (s *JobStore) applyExtension(j *Job, maxTime, add string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.Status != "running" || !isProcessAlive(j.PID) {
		return fmt.Errorf("extend only works while a job is running (job %s is %s)", j.ID, j.Status)
	}
	var total time.Duration
	if maxTime != "" {
		d, err := parseDuration(maxTime)
		if err != nil {
			return err
		}
		total = d
	} else {
		if j.Deadline.IsZero() {
			return fmt.Errorf("job %s has no time limit; set one with max_time", j.ID)
		}
		d, err := parseDuration(add)
		if err != nil {
			return err
		}
		total = j.Deadline.Add(d).Sub(j.StartTime)
	}
	if err := checkMaxTime(total); err != nil {
		return err
	}
	deadline := j.StartTime.Add(total)
	if !deadline.After(time.Now()) {
		return fmt.Errorf("new deadline %s is already in the past", deadline.UTC().Format(time.RFC3339))
	}
	j.MaxTime = formatMaxTime(total)
	j.Deadline = deadline
	s.armDeadlineLocked(j)
	return nil
}

func handleExtend(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	id := getJobID(args)
	if id == "" {
		return mcp.NewToolResultError("job_id is required"), nil
	}
	j, err := globalStore.extendDeadline(id, getStringArg(args, "max_time", ""), getStringArg(args, "add", ""))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	j.mu.RLock()
	resp := map[string]any{
		"job_id":   j.ID,
		"max_time": j.MaxTime,
		"deadline": j.Deadline.UTC().Format(time.RFC3339),
	}
	j.mu.RUnlock()
	data, err := json.Marshal(resp)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}
