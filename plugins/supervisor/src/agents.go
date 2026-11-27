package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

type agentView struct {
	AgentID      string        `json:"agent_id"`
	Source       string        `json:"source"`
	Transport    string        `json:"transport"`
	Status       string        `json:"status"`
	Name         string        `json:"name,omitempty"`
	Machine      string        `json:"machine,omitempty"`
	Host         *hostIdentity `json:"host,omitempty"`
	Harness      string        `json:"harness,omitempty"`
	Version      string        `json:"version,omitempty"`
	SessionID    string        `json:"session_id,omitempty"`
	CWD          string        `json:"cwd,omitempty"`
	Model        string        `json:"model,omitempty"`
	Title        string        `json:"title,omitempty"`
	SessionTitle string        `json:"session_title,omitempty"`
	StartTime    *time.Time    `json:"start_time,omitempty"`
	EndTime      *time.Time    `json:"end_time,omitempty"`
	PID          int           `json:"pid,omitempty"`
	JobID        string        `json:"job_id,omitempty"`

	State            string            `json:"state,omitempty"`
	Busy             *bool             `json:"busy,omitempty"`
	BusySince        *time.Time        `json:"busy_since,omitempty"`
	LastTurnEndedAt  *time.Time        `json:"last_turn_ended_at,omitempty"`
	LastTurnEndedAge *int              `json:"last_turn_ended_age_seconds,omitempty"`
	Queued           *int              `json:"queued,omitempty"`
	Steerable        bool              `json:"steerable"`
	Capabilities     []string          `json:"capabilities,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	RemoteAddr       string            `json:"remote_addr,omitempty"`
	LastSeen         string            `json:"last_seen,omitempty"`
	LastSeenAge      *int              `json:"last_seen_age_seconds,omitempty"`
	TTLSeconds       int               `json:"ttl_seconds,omitempty"`
}

func agentViewFromRegistration(ag *Agent, now time.Time) agentView {
	caps := ag.Capabilities
	if len(caps) == 0 {
		caps = []string{"prompt", "steer", "follow_up"}
	}
	queued := len(ag.Queue)
	lastSeenAge := int(now.Sub(ag.LastSeen).Seconds())
	view := agentView{
		AgentID:      ag.ID,
		Source:       "registered",
		Transport:    "poll",
		Status:       ag.status(now),
		Name:         ag.Name,
		Machine:      ag.Machine,
		Host:         ag.Host,
		Harness:      ag.Harness,
		Version:      ag.Version,
		SessionID:    ag.SessionID,
		CWD:          ag.CWD,
		Model:        ag.Model,
		Title:        ag.Title,
		SessionTitle: ag.Title,
		PID:          ag.PID,
		Queued:       &queued,
		Steerable:    true,
		Capabilities: caps,
		Labels:       ag.Labels,
		RemoteAddr:   ag.RemoteAddr,
		LastSeen:     ag.LastSeen.Format(time.RFC3339),
		LastSeenAge:  &lastSeenAge,
		TTLSeconds:   ag.TTLSeconds,
	}
	view.setTurnState(ag.turnState(now), ag.BusySince, ag.LastTurnEndedAt, now)
	return view
}

func (v *agentView) setTurnState(state string, busySince, lastTurnEnded time.Time, now time.Time) {
	v.State = state
	if state == "busy" || state == "idle" {
		busy := state == "busy"
		v.Busy = &busy
	}
	if state == "busy" && !busySince.IsZero() {
		since := busySince.UTC()
		v.BusySince = &since
	}
	if !lastTurnEnded.IsZero() {
		ended := lastTurnEnded.UTC()
		age := int(now.Sub(ended).Seconds())
		v.LastTurnEndedAt = &ended
		v.LastTurnEndedAge = &age
	}
}

func agentViewFromJob(job *Job) agentView {
	job.mu.RLock()
	defer job.mu.RUnlock()
	view := agentView{
		AgentID:      job.ID,
		Source:       "job",
		Transport:    "rpc",
		Status:       job.Status,
		Name:         job.Title,
		Machine:      job.Machine,
		Harness:      "omp",
		SessionID:    job.SessionID,
		CWD:          job.CWD,
		Model:        job.ModelResolved,
		Title:        job.Title,
		SessionTitle: job.SessionTitle,
		PID:          job.PID,
		JobID:        job.ID,
		Steerable:    job.Steerable && job.Status == "running" && job.rpc != nil && !job.rpc.isClosed(),
	}
	if !job.StartTime.IsZero() {
		start := job.StartTime
		view.StartTime = &start
	}
	if !job.EndTime.IsZero() {
		end := job.EndTime
		view.EndTime = &end
	}
	if view.Machine == "" {
		view.Machine = localMachineName()
	}
	if view.Steerable {
		view.Capabilities = []string{"prompt", "steer", "aside", "follow_up"}
	}
	state := "unknown"
	if job.Status == "running" && job.turnObserved {
		state = "idle"
		if job.turnBusy {
			state = "busy"
		}
	}
	view.setTurnState(state, job.turnBusySince, job.lastTurnEndedAt, time.Now())
	return view
}

func localMachineName() string {
	if name := strings.TrimSpace(os.Getenv("SUPERVISOR_MACHINE")); name != "" {
		return name
	}
	if name, err := os.Hostname(); err == nil && name != "" {
		return name
	}
	return "supervisor-local"
}

func listAgentViews(includeStale, includeGrok bool) ([]agentView, error) {
	now := time.Now()
	var out []agentView
	if globalAgents != nil {
		for _, ag := range globalAgents.list() {
			view := agentViewFromRegistration(ag, now)
			if view.Status != "live" && !includeStale {
				continue
			}
			out = append(out, view)
		}
	}
	if globalStore != nil {
		for _, job := range globalStore.runningSteerableJobs() {
			out = append(out, agentViewFromJob(job))
		}
	}
	if includeGrok {
		peers, err := listGrokPeerViews()
		if err != nil {
			return nil, err
		}
		out = append(out, peers...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].LastSeenAge == nil {
			return false
		}
		return out[j].LastSeenAge == nil || *out[i].LastSeenAge < *out[j].LastSeenAge
	})
	return out, nil
}

func agentViewMatches(v agentView, query string) bool {
	if query == "" {
		return true
	}
	query = strings.ToLower(query)
	fields := [13]string{v.AgentID, v.Name, v.Machine, v.Harness, v.SessionID, v.CWD, v.Model, v.Title, v.JobID, "", "", "", v.SessionTitle}
	if v.Host != nil {
		fields[9], fields[10], fields[11] = v.Host.Hostname, v.Host.Platform, v.Host.Architecture
	}
	for _, f := range fields {
		if strings.Contains(strings.ToLower(f), query) {
			return true
		}
	}
	return false
}

func handleAgents(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	query := getStringArg(args, "query", "")
	machine := strings.ToLower(getStringArg(args, "machine", ""))
	includeStale := getBoolArg(args, "include_stale", false)
	includeGrok := getBoolArg(args, "include_grok", false)
	limit := getIntArg(args, "limit", 25)
	if limit <= 0 {
		limit = 25
	}
	if limit > 100 {
		limit = 100
	}

	views, err := listAgentViews(includeStale, includeGrok)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	filtered := make([]agentView, 0, len(views))
	for _, v := range views {
		if !agentViewMatches(v, query) {
			continue
		}
		if machine != "" && !strings.Contains(strings.ToLower(v.Machine), machine) {
			continue
		}
		filtered = append(filtered, v)
	}
	total := len(filtered)
	if len(filtered) > limit {
		filtered = filtered[:limit]
	}

	resp := map[string]any{
		"agents":   filtered,
		"count":    len(filtered),
		"total":    total,
		"registry": registrySummary(),
		"hint":     "OMP is the default: prompt delivers to registered sessions or local RPC jobs. Only include_grok:true lists configured-only webhook peers; prompt with an exact grok-* ID explicitly selects one. steer/message remain OMP-only.",
	}
	return jsonToolResult(resp)
}

func registrySummary() map[string]any {
	if globalAgents == nil {
		return map[string]any{"enabled": false}
	}
	now := time.Now()
	live, stale := 0, 0
	for _, ag := range globalAgents.list() {
		if ag.status(now) == "live" {
			live++
		} else {
			stale++
		}
	}
	return map[string]any{
		"enabled":       !globalAgents.auth.disabled,
		"live":          live,
		"stale":         stale,
		"register_path": "/api/agents/register",
	}
}

func handleAgent(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	ref := agentRef(args)
	if ref == "" {
		return mcp.NewToolResultError("agent_id is required (session_id and name are accepted)"), nil
	}
	now := time.Now()

	if globalAgents != nil {
		if ag := globalAgents.resolve(ref, !isGrokPeerID(ref)); ag != nil {
			if getBoolArg(args, "revoke", false) {
				globalAgents.remove(ag.ID)
				return jsonToolResult(map[string]any{
					"agent_id": ag.ID,
					"revoked":  true,
					"note":     "registration dropped and its agent token invalidated; the client must register again",
				})
			}
			return jsonToolResult(agentDetail(ag, now, args))
		}
	}

	if job := findJobForAgentRef(ref); job != nil {
		return jsonToolResult(map[string]any{
			"agent":          agentViewFromJob(job),
			"queued":         0,
			"acked":          0,
			"history_length": 0,
		})
	}
	if isGrokPeerID(ref) {
		views, err := listGrokPeerViews()
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		for _, view := range views {
			if view.AgentID == ref {
				if getBoolArg(args, "revoke", false) {
					return mcp.NewToolResultError("configured webhook peers cannot be revoked as live registrations"), nil
				}
				return grokPeerDetail(view, args)
			}
		}
	}
	return mcp.NewToolResultError(fmt.Sprintf("no live agent matches %q; call agents to list registrations", ref)), nil
}

func grokPeerDetail(view agentView, args map[string]any) (*mcp.CallToolResult, error) {
	var asks []grokAsk
	if globalGrokAsks != nil {
		asks = globalGrokAsks.forPeer(view.AgentID)
	}
	if requestID := getStringArg(args, "request_id", ""); requestID != "" {
		for _, a := range asks {
			if a.ID == requestID {
				return jsonToolResult(map[string]any{"agent": view, "request": projectGrokAsk(a, 0)})
			}
		}
		return mcp.NewToolResultError(fmt.Sprintf("no retained ask %q for %s (asks expire after %s)", requestID, view.AgentID, grokAskRetention)), nil
	}
	answered, pending := 0, 0
	for _, a := range asks {
		switch grokAskState(a.Status) {
		case "answered":
			answered++
		case "pending":
			pending++
		}
	}
	resp := map[string]any{
		"agent":          view,
		"queued":         0,
		"pending":        pending,
		"acked":          answered,
		"history_length": len(asks),
	}
	if len(asks) > 0 {
		previews := make([]map[string]any, 0, agentPreviewLimit)
		for _, a := range asks[:min(len(asks), agentPreviewLimit)] {
			previews = append(previews, projectGrokAsk(a, previewMaxChars))
		}
		resp["history_preview"] = previews
	}
	if getBoolArg(args, "include_history", false) {
		maxChars := clampMaxChars(getIntArg(args, "max_chars", defaultMaxChars))
		requests := make([]map[string]any, 0, len(asks))
		for _, a := range asks {
			requests = append(requests, projectGrokAsk(a, maxChars))
		}
		resp["requests"] = requests
	}
	return jsonToolResult(resp)
}

func agentDetail(ag *Agent, now time.Time, args map[string]any) map[string]any {
	includeQueue := getBoolArg(args, "include_queue", false)
	includeHistory := getBoolArg(args, "include_history", false)
	maxChars := clampMaxChars(getIntArg(args, "max_chars", defaultMaxChars))

	acked := 0
	for _, d := range ag.History {
		if d.Status == "acked" {
			acked++
		}
	}

	resp := map[string]any{
		"agent":          agentViewFromRegistration(ag, now),
		"queued":         len(ag.Queue),
		"acked":          acked,
		"history_length": len(ag.History),
	}
	if previews := projectDeliveries(ag.Queue, previewMaxChars, agentPreviewLimit, false); len(previews) > 0 {
		resp["queue_preview"] = previews
	}
	if previews := projectDeliveries(ag.History, previewMaxChars, agentPreviewLimit, true); len(previews) > 0 {
		resp["history_preview"] = previews
	}
	if includeQueue {
		resp["queue"] = projectDeliveries(ag.Queue, maxChars, 0, false)
		if resp["queue"] == nil {
			resp["queue"] = []map[string]any{}
		}
	}
	if includeHistory {
		resp["deliveries"] = projectDeliveries(ag.History, maxChars, 0, false)
		if resp["deliveries"] == nil {
			resp["deliveries"] = []map[string]any{}
		}
	}
	return resp
}

func projectDelivery(d *Delivery, maxChars int, preview bool) map[string]any {
	text, tTrunc := headCut(d.Text, maxChars)
	result, rTrunc := headCut(d.Result, maxChars)
	errText, eTrunc := headCut(d.Error, maxChars)
	out := map[string]any{
		"id":     d.ID,
		"mode":   d.Mode,
		"status": d.Status,
	}
	if d.From != "" {
		out["from"] = d.From
	}
	if d.Source != nil {
		out["source"] = d.Source
	}
	if preview && result != "" {

		out["result"] = result
		out["result_chars"] = len(d.Result)
	} else {
		if text != "" {
			out["text"] = text
			out["text_chars"] = len(d.Text)
		}
		if result != "" {
			out["result"] = result
			out["result_chars"] = len(d.Result)
		}
	}
	if errText != "" {
		out["error"] = errText
	}
	if tTrunc || rTrunc || eTrunc {
		out["truncated"] = true
	}
	if !preview {
		if !d.CreatedAt.IsZero() {
			out["created_at"] = d.CreatedAt
		}
		if !d.DeliveredAt.IsZero() {
			out["delivered_at"] = d.DeliveredAt
		}
		if !d.AckedAt.IsZero() {
			out["acked_at"] = d.AckedAt
		}
	}
	return out
}

func projectDeliveries(list []*Delivery, maxChars, limit int, fromEnd bool) []map[string]any {
	if len(list) == 0 {
		return nil
	}
	start, end := 0, len(list)
	if limit > 0 && len(list) > limit {
		if fromEnd {
			start = len(list) - limit
		} else {
			end = limit
		}
	}
	preview := limit > 0
	out := make([]map[string]any, 0, end-start)
	for _, d := range list[start:end] {
		out = append(out, projectDelivery(d, maxChars, preview))
	}
	return out
}

func agentRef(args map[string]any) string {
	for _, key := range []string{"agent_id", "agent", "id", "session_id", "name"} {
		if v := getStringArg(args, key, ""); v != "" {
			return v
		}
	}
	return ""
}

func findJobForAgentRef(ref string) *Job {
	if globalStore == nil {
		return nil
	}
	if job := globalStore.get(ref); job != nil {
		return job
	}
	if job := globalStore.findRunningJobBySessionID(ref); job != nil {
		return job
	}
	return nil
}

func handlePrompt(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return handleAgentPrompt(ctx, request, true)
}

func handleAgentPrompt(ctx context.Context, request mcp.CallToolRequest, allowGrok bool) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	source, err := promptSourceArg(args)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	ref := agentRef(args)
	if ref == "" {
		return mcp.NewToolResultError("agent_id is required (call agents to list live agents)"), nil
	}
	text := firstNonEmptyArg(args, "text", "message", "prompt")
	if text == "" {
		return mcp.NewToolResultError("text is required (message and prompt are accepted aliases)"), nil
	}
	mode, err := normalizeDeliveryMode(getStringArg(args, "mode", "steer"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	senderSession := strings.TrimSpace(getStringArg(args, "sender_session_id", ""))

	if job := findJobForAgentRef(ref); job != nil {
		job.mu.RLock()
		targetSession := job.SessionID
		job.mu.RUnlock()
		if senderSession != "" && targetSession == senderSession {
			return mcp.NewToolResultError(selfTargetError(job.ID, targetSession)), nil
		}
		resp, err := steerJob(job.ID, mode, text, source)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		resp["agent_id"] = job.ID
		resp["source"] = "job"
		resp["transport"] = "rpc"
		return jsonToolResult(resp)
	}

	var ag *Agent
	if globalAgents != nil {
		ag = globalAgents.resolve(ref, !isGrokPeerID(ref))
	}
	if ag == nil {
		if allowGrok {
			peer, err := resolveGrokPeer(ref)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			if peer != nil {
				waitSeconds := getIntArg(args, "wait_seconds", 120)
				if waitSeconds <= 0 {
					waitSeconds = 120
				}
				wait := time.Duration(waitSeconds) * time.Second
				if strings.EqualFold(getStringArg(args, "wait_for", ""), "none") {
					wait = 0
				}
				res, err := askGrokPeer(ctx, *peer, text, getStringArg(args, "from", ""), source, wait)
				if res == nil {
					return mcp.NewToolResultError(err.Error()), nil
				}
				resp := map[string]any{
					"agent_id":   res.Peer,
					"source":     "grok",
					"transport":  res.Transport,
					"request_id": res.Ask.ID,
					"status":     res.Ask.Status,
					"state":      grokAskState(res.Ask.Status),
					"delivered":  res.Ask.Status == grokAskDelivered || res.Ask.Status == grokAskAnswered,
				}
				switch res.Ask.Status {
				case grokAskAnswered:
					resp["result"] = res.Ask.Answer
				case grokAskFailed:
					resp["note"] = "the webhook rejected the ask; it was not delivered"
				case grokAskUnconfirmed:
					resp["expires_at"] = res.Ask.expiresAt()
					resp["note"] = fmt.Sprintf("delivery is unconfirmed; the ask stays open and an answer that arrives is stored. Check agent{agent_id: %q, request_id: %q} before resending.", res.Peer, res.Ask.ID)
				default:
					resp["expires_at"] = res.Ask.expiresAt()
					resp["note"] = fmt.Sprintf("no answer yet; the ask stays open and a late answer is stored. Retrieve it with agent{agent_id: %q, request_id: %q} instead of resending.", res.Peer, res.Ask.ID)
				}
				if err != nil {

					resp["error"] = err.Error()
					raw, mErr := json.Marshal(resp)
					if mErr != nil {
						return mcp.NewToolResultError(err.Error()), nil
					}
					return mcp.NewToolResultError(string(raw)), nil
				}
				return jsonToolResult(resp)
			}
		}
		return mcp.NewToolResultError(fmt.Sprintf("no live OMP agent matches %q; prompt with an exact configured grok-* ID is required for a webhook", ref)), nil
	}
	if senderSession != "" && ag.SessionID == senderSession {
		return mcp.NewToolResultError(selfTargetError(ag.ID, ag.SessionID)), nil
	}
	if status := ag.status(time.Now()); status != "live" {
		return mcp.NewToolResultError(fmt.Sprintf("agent %s is %s (last heartbeat %ds ago); it cannot receive prompts until it heartbeats again", ag.ID, status, int(time.Since(ag.LastSeen).Seconds()))), nil
	}

	delivery, err := globalAgents.enqueue(ag.ID, text, mode, getStringArg(args, "from", ""), source)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("could not queue prompt for %s: %v", ag.ID, err)), nil
	}

	waitFor := strings.ToLower(getStringArg(args, "wait_for", "delivered"))
	waitSeconds := getIntArg(args, "wait_seconds", 10)
	final := waitForDelivery(ctx, ag.ID, delivery.ID, waitFor, time.Duration(waitSeconds)*time.Second)

	resp := map[string]any{
		"agent_id":    ag.ID,
		"source":      "registered",
		"transport":   "poll",
		"delivery_id": delivery.ID,
		"mode":        mode,
		"status":      final.Status,
		"delivered":   final.Status != "queued",
		"machine":     ag.Machine,
		"session_id":  ag.SessionID,
	}
	if final.Result != "" {
		cut, trunc := headCut(final.Result, defaultMaxChars)
		resp["result"] = cut
		if trunc {
			resp["result_chars"] = len(final.Result)
			resp["truncated"] = true
		}
	}
	if final.Error != "" {
		cut, trunc := headCut(final.Error, defaultMaxChars)
		resp["error"] = cut
		if trunc {
			resp["truncated"] = true
		}
	}
	if final.Status == "queued" {
		resp["note"] = fmt.Sprintf("queued; %s picks it up on its next inbox poll. Call agent{agent_id} to see when it lands.", ag.ID)
	}
	return jsonToolResult(resp)
}

func selfTargetError(id, sessionID string) string {
	return fmt.Sprintf("refused: %s is your own session (session_id %s); a session cannot prompt itself. Pick a different agent from agents", id, sessionID)
}

func waitForDelivery(ctx context.Context, agentID, deliveryID, waitFor string, timeout time.Duration) Delivery {
	current := Delivery{ID: deliveryID, Status: "queued"}
	if waitFor == "none" || timeout <= 0 {
		if d := globalAgents.findDelivery(agentID, deliveryID); d != nil {
			return *d
		}
		return current
	}
	if timeout > 5*time.Minute {
		timeout = 5 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	for {
		if d := globalAgents.findDelivery(agentID, deliveryID); d != nil {
			current = *d
			done := d.Status == "acked" || d.Status == "failed"
			if waitFor != "ack" {
				done = done || d.Status == "delivered"
			}
			if done {
				return current
			}
		}
		if time.Now().After(deadline) {
			return current
		}
		select {
		case <-ctx.Done():
			return current
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func normalizeDeliveryMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "steer":
		return "steer", nil
	case "aside":
		return "aside", nil
	case "follow_up", "followup", "follow-up":
		return "follow_up", nil
	}
	return "", fmt.Errorf("unsupported mode %q: use steer (interrupt), aside (peer input without aborting), or follow_up (after the current turn)", mode)
}

func firstNonEmptyArg(args map[string]any, keys ...string) string {
	for _, key := range keys {
		if v := getStringArg(args, key, ""); v != "" {
			return v
		}
	}
	return ""
}

func jsonToolResult(payload map[string]any) (*mcp.CallToolResult, error) {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
	}
	return mcp.NewToolResultText(string(jsonBytes)), nil
}
