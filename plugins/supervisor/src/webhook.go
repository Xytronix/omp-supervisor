package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const maxAskRunes = 100000

type GrokTarget struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	URL  string `json:"url"`
	Key  string `json:"key"`
}

func isGrokPeerID(id string) bool {
	if !strings.HasPrefix(id, "grok-") || len(id) == len("grok-") {
		return false
	}
	separator := true
	for _, char := range id[len("grok-"):] {
		if char == '-' {
			if separator {
				return false
			}
			separator = true
		} else if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' {
			separator = false
		} else {
			return false
		}
	}
	return !separator
}

func loadCompletionWebhookTargets() ([]GrokTarget, error) {
	raw := strings.TrimSpace(os.Getenv("SUPERVISOR_COMPLETION_WEBHOOKS"))
	if raw == "" {
		return nil, nil
	}
	var targets []GrokTarget
	if err := json.Unmarshal([]byte(raw), &targets); err != nil || targets == nil {
		return nil, fmt.Errorf("SUPERVISOR_COMPLETION_WEBHOOKS must be a JSON array of id/name/url/key objects")
	}
	type endpoint struct{ url, key string }
	byID := make(map[string]GrokTarget, len(targets))
	byEndpoint := make(map[endpoint]string, len(targets))
	out := make([]GrokTarget, 0, len(targets))
	for i, target := range targets {
		if !isGrokPeerID(target.ID) {
			return nil, fmt.Errorf("SUPERVISOR_COMPLETION_WEBHOOKS entry %d requires an exact canonical grok-* id", i+1)
		}
		target.Name = strings.TrimSpace(target.Name)
		target.URL = strings.TrimSpace(target.URL)
		target.Key = strings.TrimSpace(target.Key)
		if target.URL == "" || target.Key == "" {
			return nil, fmt.Errorf("SUPERVISOR_COMPLETION_WEBHOOKS entry %d requires url and key", i+1)
		}
		if previous, exists := byID[target.ID]; exists {
			if previous != target {
				return nil, fmt.Errorf("SUPERVISOR_COMPLETION_WEBHOOKS has conflicting registrations for %s", target.ID)
			}
			continue
		}
		address := endpoint{target.URL, target.Key}
		if owner, exists := byEndpoint[address]; exists {
			return nil, fmt.Errorf("SUPERVISOR_COMPLETION_WEBHOOKS assigns one endpoint/key to both %s and %s", owner, target.ID)
		}
		byID[target.ID] = target
		byEndpoint[address] = target.ID
		out = append(out, target)
	}
	return out, nil
}

func truncateRunes(s string, max int) string {
	if max <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

func completionAsk(id, status, title, report string) string {
	body := strings.TrimSpace(report)
	if body == "" {
		body = strings.TrimSpace(title)
	}
	if body == "" {
		body = "(no report)"
	}
	body = truncateRunes(body, maxAskRunes)
	var b strings.Builder
	b.WriteString("Supervisor job ")
	b.WriteString(id)
	b.WriteString(" finished with status ")
	b.WriteString(status)
	b.WriteString(".\n\n")
	if t := strings.TrimSpace(title); t != "" {
		b.WriteString("Title: ")
		b.WriteString(t)
		b.WriteString("\n\n")
	}
	b.WriteString("Report / result:\n")
	b.WriteString(body)
	b.WriteString("\n\nSummarize for the user in chat. Do not re-run the job unless the report explicitly asks for follow-up work.")
	return b.String()
}

func buildCompletionPayload(job *Job) map[string]any {
	job.mu.RLock()
	id := job.ID
	status := job.Status
	exitCode := job.ExitCode
	title := job.Title
	sessionTitle := job.SessionTitle
	model := job.Model
	jobPrompt := job.Prompt
	report := job.Report
	sessionID := job.SessionID
	cwd := job.CWD
	start := job.StartTime
	end := job.EndTime
	job.mu.RUnlock()

	ask := completionAsk(id, status, title, report)
	payload := map[string]any{
		"source":     "supervisor",
		"event":      "job.terminal",
		"id":         id,
		"status":     status,
		"exit_code":  exitCode,
		"title":      title,
		"model":      model,
		"job_prompt": truncateRunes(jobPrompt, maxAskRunes),
		"report":     truncateRunes(report, maxAskRunes),
		"session_id": sessionID,
		"cwd":        cwd,

		"prompt":  ask,
		"message": ask,
		"text":    ask,
	}
	if sessionTitle != "" {
		payload["session_title"] = sessionTitle
	}
	if !start.IsZero() {
		payload["start_time"] = start
	}
	if !end.IsZero() {
		payload["end_time"] = end
	}
	return payload
}

func validateCompletionNotification(enabled bool, parentAgentID string) error {
	if !enabled {
		return nil
	}
	target, err := resolveGrokPeer(parentAgentID)
	if err != nil {
		return err
	}
	if target == nil {
		return fmt.Errorf("notify_on_completion requires parent_agent_id to be an exact configured grok-* id")
	}
	return nil
}

func (job *Job) completionWebhook() *GrokTarget {
	if job == nil {
		return nil
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	if !job.NotifyOnCompletion || job.completionNotified {
		return nil
	}
	switch job.Status {
	case "done", "error", "canceled":
	default:
		return nil
	}
	target, err := resolveGrokPeer(job.ParentAgentID)
	if err != nil {
		log.Printf("supervisor webhooks: %v", err)
		return nil
	}
	if target != nil {
		job.completionNotified = true
	}
	return target
}

func notifyCompletionWebhooks(job *Job) {
	target := job.completionWebhook()
	if target == nil {
		return
	}
	body, err := json.Marshal(buildCompletionPayload(job))
	if err != nil {
		log.Printf("supervisor webhooks: marshal: %v", err)
		return
	}
	go func() {
		req, err := http.NewRequest(http.MethodPost, target.URL, bytes.NewReader(body))
		if err != nil {
			log.Printf("supervisor webhook %s: request: %v", target.Name, err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+target.Key)
		client := &http.Client{Timeout: 15 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("supervisor webhook %s: post: %v", target.Name, err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Printf("supervisor webhook %s: HTTP %d", target.Name, resp.StatusCode)
			return
		}
		log.Printf("supervisor webhook %s: ok (%d)", target.Name, resp.StatusCode)
	}()
}
