package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

func normalizeSessionID(id string) string {
	id = strings.TrimSpace(id)
	if len(id) >= 4 && strings.EqualFold(id[:4], "omp:") {
		return strings.TrimSpace(id[4:])
	}
	return id
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	if h, err := os.UserHomeDir(); err == nil && h != "" {
		return h
	}
	return os.TempDir()
}

func sessionsBaseDir() string {
	if d := os.Getenv("OMP_SESSIONS_DIR"); d != "" {
		return d
	}
	return filepath.Join(homeDir(), ".omp", "agent", "sessions")
}

type ompModel struct {
	Provider      string   `json:"provider"`
	ID            string   `json:"id"`
	Selector      string   `json:"selector"`
	Name          string   `json:"name,omitempty"`
	ContextWindow int      `json:"context_window,omitempty"`
	MaxTokens     int      `json:"max_tokens,omitempty"`
	Thinking      []string `json:"thinking,omitempty"`
	Images        bool     `json:"images,omitempty"`
}

type modelProvider struct {
	Provider string `json:"provider"`
	Count    int    `json:"count"`
}

type ompModelsFile struct {
	Models []struct {
		Provider      string   `json:"provider"`
		ID            string   `json:"id"`
		Selector      string   `json:"selector"`
		Name          string   `json:"name"`
		ContextWindow int      `json:"contextWindow"`
		MaxTokens     int      `json:"maxTokens"`
		Thinking      []string `json:"thinking"`
		Input         []string `json:"input"`
	} `json:"models"`
}

func parseOmpModelsJSON(raw []byte) []ompModel {
	var f ompModelsFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil
	}
	out := make([]ompModel, 0, len(f.Models))
	for _, m := range f.Models {
		selector := m.Selector
		if selector == "" && m.Provider != "" && m.ID != "" {
			selector = m.Provider + "/" + m.ID
		}
		images := false
		for _, in := range m.Input {
			if strings.EqualFold(in, "image") {
				images = true
				break
			}
		}
		out = append(out, ompModel{
			Provider:      m.Provider,
			ID:            m.ID,
			Selector:      selector,
			Name:          m.Name,
			ContextWindow: m.ContextWindow,
			MaxTokens:     m.MaxTokens,
			Thinking:      m.Thinking,
			Images:        images,
		})
	}
	return out
}

var (
	modelsCacheMu sync.Mutex
	modelsCache   []ompModel
	modelsCacheAt time.Time
)

func setModelsCatalogForTest(models []ompModel) {
	modelsCacheMu.Lock()
	defer modelsCacheMu.Unlock()
	modelsCache = models
	modelsCacheAt = time.Now()
}

func runOmpModelsJSON(extraArgs ...string) ([]byte, error) {
	args := append([]string{"models"}, extraArgs...)
	return runOmp(append(args, "--json")...)
}

func runOmp(args ...string) ([]byte, error) {
	bin, err := locateOmpBinary()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = os.TempDir()
	cmd.Env = append(os.Environ(), "HOME="+homeDir())
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return nil, fmt.Errorf("omp %s failed: %s", args[0], sanitizeSecretString(msg))
	}
	return stdout.Bytes(), nil
}

func loadModelsCatalog(force bool) ([]ompModel, error) {
	modelsCacheMu.Lock()
	defer modelsCacheMu.Unlock()
	if !force && time.Since(modelsCacheAt) < 5*time.Minute && len(modelsCache) > 0 {
		return modelsCache, nil
	}
	raw, err := runOmpModelsJSON()
	if err != nil {
		if len(modelsCache) > 0 {
			return modelsCache, nil
		}
		return nil, err
	}
	modelsCache = parseOmpModelsJSON(raw)
	modelsCacheAt = time.Now()
	return modelsCache, nil
}

func aggregateProviders(models []ompModel) []modelProvider {
	counts := map[string]int{}
	for _, m := range models {
		if m.Provider == "" {
			continue
		}
		counts[m.Provider]++
	}
	out := make([]modelProvider, 0, len(counts))
	for p, n := range counts {
		out = append(out, modelProvider{Provider: p, Count: n})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Provider < out[j].Provider
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func filterModels(models []ompModel, query, provider string, limit int) []ompModel {
	q := strings.ToLower(strings.TrimSpace(query))
	p := strings.ToLower(strings.TrimSpace(provider))
	if limit <= 0 {
		limit = 25
	}
	out := make([]ompModel, 0, limit)
	for _, m := range models {
		if p != "" && !strings.Contains(strings.ToLower(m.Provider), p) {
			continue
		}
		if q != "" {
			blob := strings.ToLower(strings.Join([]string{m.Provider, m.ID, m.Selector, m.Name}, " "))
			if !strings.Contains(blob, q) {
				continue
			}
		}
		out = append(out, m)
		if len(out) >= limit {
			break
		}
	}
	return out
}

type sessionSummary struct {
	SessionID string `json:"session_id"`
	Title     string `json:"title"`
	CWD       string `json:"cwd,omitempty"`
	Model     string `json:"model,omitempty"`
	Thinking  string `json:"thinking,omitempty"`
	Fallback  bool   `json:"fallback,omitempty"`
	Snippet   string `json:"snippet,omitempty"`
	MTime     string `json:"mtime"`
	Size      int64  `json:"size"`
	Path      string `json:"-"`
}

type convoMessage struct {
	Role string `json:"role"`
	Text string `json:"text"`
}

func extractPlainText(content any) string {
	switch v := content.(type) {
	case string:
		return v
	case []any:
		var b strings.Builder
		for _, item := range v {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			typ, _ := m["type"].(string)
			if typ != "text" {
				continue
			}
			if t, ok := m["text"].(string); ok && t != "" {
				if b.Len() > 0 {
					b.WriteByte('\n')
				}
				b.WriteString(t)
			}
		}
		return b.String()
	default:
		return ""
	}
}

func collapseWS(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func clip(s string, n int) string {
	s = strings.TrimSpace(s)
	if n <= 0 || len(s) <= n {
		return s
	}
	return s[:n]
}

func firstLineTitle(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.Index(s, "# "); i >= 0 && i < 240 {
		rest := strings.TrimSpace(s[i+2:])
		if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
			rest = rest[:nl]
		}
		rest = strings.TrimSpace(rest)
		if rest != "" {
			if len(rest) > 80 {
				return rest[:77] + "..."
			}
			return rest
		}
	}
	if nl := strings.IndexByte(s, '\n'); nl >= 0 {
		s = s[:nl]
	}
	s = strings.TrimSpace(s)
	if len(s) > 80 {
		return s[:77] + "..."
	}
	return s
}

func summarizeSessionFile(path string, info fs.FileInfo) sessionSummary {
	sum := sessionSummary{
		Path:  path,
		Size:  info.Size(),
		MTime: info.ModTime().UTC().Format(time.RFC3339),
	}
	base := strings.TrimSuffix(filepath.Base(path), ".jsonl")
	if i := strings.LastIndex(base, "_"); i >= 0 && i+1 < len(base) {
		sum.SessionID = base[i+1:]
	}
	f, err := os.Open(path)
	if err != nil {
		return sum
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	buf := make([]byte, 1024*1024)
	sc.Buffer(buf, 1024*1024)
	lines := 0
	for sc.Scan() {
		lines++
		if lines > 80 && sum.Title != "" && sum.SessionID != "" && sum.Model != "" && sum.Snippet != "" {
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		switch ev["type"] {
		case "title":
			if t, ok := ev["title"].(string); ok && strings.TrimSpace(t) != "" {
				sum.Title = strings.TrimSpace(t)
			}
		case "session":
			if id, ok := ev["id"].(string); ok && id != "" {
				sum.SessionID = id
			}
			if cwd, ok := ev["cwd"].(string); ok {
				sum.CWD = cwd
			}
		case "model_change":
			if m, ok := ev["model"].(string); ok && m != "" {
				sum.Model = m
			}
			if fb, ok := ev["resolvedModelIsFallback"].(bool); ok {
				sum.Fallback = fb
			}
		case "thinking_level_change":
			if t, ok := ev["thinkingLevel"].(string); ok {
				sum.Thinking = t
			}
		case "message":
			if sum.Snippet != "" {
				continue
			}
			msg, _ := ev["message"].(map[string]any)
			if msg == nil {
				continue
			}
			role, _ := msg["role"].(string)
			if role != "user" {
				continue
			}
			text := sanitizeSecretString(extractPlainText(msg["content"]))
			if sum.Title == "" {
				sum.Title = firstLineTitle(text)
			}
			sum.Snippet = clip(collapseWS(text), 240)
		}
	}
	if sum.Title == "" {
		sum.Title = sum.SessionID
	}
	return sum
}

func collectSessionFiles(base string) []string {
	var files []string
	entries, err := os.ReadDir(base)
	if err != nil {
		return nil
	}
	for _, de := range entries {
		p := filepath.Join(base, de.Name())
		if !de.IsDir() {
			if strings.HasSuffix(de.Name(), ".jsonl") {
				files = append(files, p)
			}
			continue
		}
		kids, err := os.ReadDir(p)
		if err != nil {
			continue
		}
		for _, k := range kids {
			if !k.IsDir() && strings.HasSuffix(k.Name(), ".jsonl") {
				files = append(files, filepath.Join(p, k.Name()))
			}
		}
	}
	return files
}

func listLocalSessions(query, cwd string, limit int) []sessionSummary {
	if limit <= 0 {
		limit = 20
	}
	files := collectSessionFiles(sessionsBaseDir())
	type dated struct {
		path string
		info fs.FileInfo
	}
	datedFiles := make([]dated, 0, len(files))
	for _, p := range files {
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		datedFiles = append(datedFiles, dated{p, info})
	}
	sort.Slice(datedFiles, func(i, j int) bool {
		return datedFiles[i].info.ModTime().After(datedFiles[j].info.ModTime())
	})
	q := strings.ToLower(strings.TrimSpace(query))
	cwdF := strings.ToLower(strings.TrimSpace(cwd))
	out := make([]sessionSummary, 0, limit)
	maxScan := 400
	for i, df := range datedFiles {
		if i >= maxScan {
			break
		}
		sum := summarizeSessionFile(df.path, df.info)
		if cwdF != "" && !strings.Contains(strings.ToLower(sum.CWD), cwdF) {
			continue
		}
		if q != "" {
			blob := strings.ToLower(strings.Join([]string{sum.SessionID, sum.Title, sum.CWD, sum.Model, sum.Snippet}, " "))
			if !strings.Contains(blob, q) {
				continue
			}
		}
		out = append(out, sum)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func validSessionID(id string) bool {
	if len(id) < 8 || len(id) > 64 {
		return false
	}
	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-') {
			return false
		}
	}
	return true
}

func findSessionFileByID(base, sessionID string) string {
	suffix := "_" + sessionID + ".jsonl"
	for _, p := range collectSessionFiles(base) {
		if strings.HasSuffix(filepath.Base(p), suffix) {
			return p
		}
	}
	return ""
}

func clampConversationLimits(maxMessages, maxChars int) (int, int) {
	if maxMessages <= 0 {
		maxMessages = 20
	}
	if maxMessages > 40 {
		maxMessages = 40
	}
	if maxChars <= 0 {
		maxChars = 4000
	}
	if maxChars > 8000 {
		maxChars = 8000
	}
	return maxMessages, maxChars
}

var errSessionNotLocal = errors.New("session not found on this host")

func readSessionConversation(sessionID string, maxMessages, maxChars int) (sessionSummary, []convoMessage, error) {
	sessionID = normalizeSessionID(sessionID)
	if sessionID == "" {
		return sessionSummary{}, nil, fmt.Errorf("session_id is required")
	}
	if !validSessionID(sessionID) {
		return sessionSummary{}, nil, fmt.Errorf("session_id %q is not an OMP session id", sessionID)
	}
	path := findSessionFileByID(sessionsBaseDir(), sessionID)
	if path == "" {
		return sessionSummary{}, nil, errSessionNotLocal
	}
	info, err := os.Stat(path)
	if err != nil {
		return sessionSummary{}, nil, fmt.Errorf("session %q not readable: %w", sessionID, err)
	}
	sum := summarizeSessionFile(path, info)
	if sum.SessionID != sessionID {
		return sessionSummary{}, nil, fmt.Errorf("session file %s belongs to session %q, not %q", filepath.Base(path), sum.SessionID, sessionID)
	}
	maxMessages, maxChars = clampConversationLimits(maxMessages, maxChars)
	f, err := os.Open(path)
	if err != nil {
		return sum, nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	buf := make([]byte, 1024*1024)
	sc.Buffer(buf, 16*1024*1024)
	msgs := make([]convoMessage, 0, maxMessages)
	used := 0
	for sc.Scan() {
		if len(msgs) >= maxMessages || used >= maxChars {
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" || !strings.Contains(line, `"type":"message"`) {
			continue
		}
		var ev struct {
			Type    string `json:"type"`
			Message struct {
				Role    string `json:"role"`
				Content any    `json:"content"`
			} `json:"message"`
		}
		if err := json.Unmarshal([]byte(line), &ev); err != nil || ev.Type != "message" {
			continue
		}
		role := ev.Message.Role
		if role != "user" && role != "assistant" {
			continue
		}
		text := sanitizeSecretString(extractPlainText(ev.Message.Content))
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		remain := maxChars - used
		if remain <= 0 {
			break
		}
		if len(text) > remain {
			text = text[:remain]
		}
		msgs = append(msgs, convoMessage{Role: role, Text: text})
		used += len(text)
	}
	return sum, msgs, nil
}

func clipConversation(in []convoMessage, maxMessages, maxChars int) []convoMessage {
	maxMessages, maxChars = clampConversationLimits(maxMessages, maxChars)
	out := make([]convoMessage, 0, min(len(in), maxMessages))
	used := 0
	for _, m := range in {
		if len(out) >= maxMessages || used >= maxChars {
			break
		}
		if m.Role != "user" && m.Role != "assistant" {
			continue
		}
		text := strings.TrimSpace(sanitizeSecretString(m.Text))
		if text == "" {
			continue
		}
		if remain := maxChars - used; len(text) > remain {
			text = text[:remain]
		}
		out = append(out, convoMessage{Role: m.Role, Text: text})
		used += len(text)
	}
	return out
}

func sessionLocation(sessionID string) (machine, via string) {
	if globalAgents != nil {
		for _, ag := range globalAgents.list() {
			if ag.SessionID == sessionID && ag.Machine != "" {
				return ag.Machine, "registered agent " + ag.ID
			}
		}
	}
	if globalStore != nil {
		globalStore.mu.RLock()
		defer globalStore.mu.RUnlock()
		var newest *Job
		var newestStart time.Time
		for _, j := range globalStore.jobs {
			j.mu.RLock()
			if j.SessionID == sessionID && (newest == nil || j.StartTime.After(newestStart)) {
				newest, newestStart = j, j.StartTime
				machine = j.Machine
			}
			j.mu.RUnlock()
		}
		if newest != nil {
			if machine == "" {
				machine = localMachineName()
			}
			return machine, "job " + newest.ID
		}
	}
	return "", ""
}

func remoteSessionConversation(ctx context.Context, target *remoteMachine, sessionID string, maxMessages, maxChars int) (sessionSummary, []convoMessage, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cmd, err := remoteCommand(ctx, target)
	if err != nil {
		return sessionSummary{}, nil, false, err
	}
	maxMessages, maxChars = clampConversationLimits(maxMessages, maxChars)
	request, _ := json.Marshal(map[string]any{"op": "session", "max_time": "session", "session_id": sessionID, "max_messages": maxMessages, "max_chars": maxChars})
	cmd.Stdin = bytes.NewReader(append(request, '\n'))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	scanner := bufio.NewScanner(&stdout)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var reply struct {
			Type     string         `json:"type"`
			Found    bool           `json:"found"`
			Error    string         `json:"error"`
			Session  sessionSummary `json:"session"`
			Messages []convoMessage `json:"messages"`
		}
		if json.Unmarshal(scanner.Bytes(), &reply) != nil || reply.Type != "session" {
			continue
		}
		if reply.Error != "" {
			return sessionSummary{}, nil, false, errors.New(sanitizeSecretString(reply.Error))
		}
		if !reply.Found {
			return sessionSummary{}, nil, false, nil
		}
		if reply.Session.SessionID != sessionID {
			return sessionSummary{}, nil, false, fmt.Errorf("host returned session %q for %q", reply.Session.SessionID, sessionID)
		}
		sum := reply.Session
		sum.Title = sanitizeSecretString(sum.Title)
		return sum, clipConversation(reply.Messages, maxMessages, maxChars), true, nil
	}
	if strings.Contains(stderr.String(), "Remote launch rejected") {
		return sessionSummary{}, nil, false, fmt.Errorf("its omp-rpc-host.mjs predates session reads; deploy the current client/omp-rpc-host.mjs there")
	}
	if runErr != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = runErr.Error()
		}
		return sessionSummary{}, nil, false, errors.New(sanitizeSecretString(msg))
	}
	return sessionSummary{}, nil, false, fmt.Errorf("its omp-rpc-host.mjs gave no session answer")
}

func readSessionAnywhere(ctx context.Context, rawID, machine string, maxMessages, maxChars int) (string, sessionSummary, []convoMessage, error) {
	sessionID := normalizeSessionID(rawID)
	if sessionID == "" {
		return "", sessionSummary{}, nil, fmt.Errorf("session_id is required")
	}
	if !validSessionID(sessionID) {
		return "", sessionSummary{}, nil, fmt.Errorf("session_id %q is not an OMP session id", sessionID)
	}
	local := localMachineName()
	via := "machine argument"
	if machine == "" {
		machine, via = sessionLocation(sessionID)
	}
	if machine == "" || machine == local {
		sum, msgs, err := readSessionConversation(sessionID, maxMessages, maxChars)
		if errors.Is(err, errSessionNotLocal) {
			return local, sessionSummary{}, nil, fmt.Errorf("session %s was not found on the Supervisor host %s and no live agent or job records which machine holds it; pass machine (see machines) to read it there", sessionID, local)
		}
		return local, sum, msgs, err
	}
	targets, err := loadRemoteMachines()
	if err != nil {
		return machine, sessionSummary{}, nil, fmt.Errorf("session %s lives on machine %s (%s), but the machine allowlist is unreadable: %v", sessionID, machine, via, err)
	}
	target, ok := targets[machine]
	if !ok {
		return machine, sessionSummary{}, nil, fmt.Errorf("session %s lives on machine %s (%s), which is not a configured launch target; Supervisor cannot read it", sessionID, machine, via)
	}
	sum, msgs, found, err := remoteSessionConversation(ctx, &target, sessionID, maxMessages, maxChars)
	if err != nil {
		return machine, sessionSummary{}, nil, fmt.Errorf("session %s lives on machine %s (%s) and could not be read there: %v", sessionID, machine, via, err)
	}
	if !found {
		return machine, sessionSummary{}, nil, fmt.Errorf("session %s was not found on machine %s (%s)", sessionID, machine, via)
	}
	return machine, sum, msgs, nil
}

func toolJSON(v any) *mcp.CallToolResult {
	b, err := json.Marshal(v)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err))
	}
	return mcp.NewToolResultText(string(b))
}

func handleModels(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	query := getStringArg(args, "query", "")
	provider := getStringArg(args, "provider", "")
	limit := getIntArg(args, "limit", 25)
	if limit <= 0 {
		limit = 25
	}
	if limit > 50 {
		limit = 50
	}
	alias, target, err := modelsMachine(getStringArg(args, "machine", ""))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	inventory, inventoryErr := modelsForMachine(ctx, alias, target, target != nil)
	var catalog []ompModel
	if target == nil {
		if catalog, err = loadModelsCatalog(false); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
	} else {
		if inventoryErr != nil {
			return mcp.NewToolResultError(fmt.Sprintf("machine %s: %v", alias, inventoryErr)), nil
		}
		catalog = inventory.Catalog
	}
	out := map[string]any{"machine": alias}
	if inventoryErr == nil {
		out["roles"] = inventory.Roles
		out["fallback_chains"] = inventory.FallbackChains
	} else {
		out["roles_error"] = inventoryErr.Error()
	}
	if query == "" && provider == "" {
		out["providers"] = aggregateProviders(catalog)
		out["hint"] = "pass query or provider to list selectable model IDs"
		return toolJSON(out), nil
	}
	out["models"] = filterModels(catalog, query, provider, limit)
	return toolJSON(out), nil
}

func handleSessions(_ context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	query := getStringArg(args, "query", "")
	cwd := getStringArg(args, "cwd", "")
	limit := getIntArg(args, "limit", 20)
	if limit <= 0 {
		limit = 20
	}
	if limit > 50 {
		limit = 50
	}
	return toolJSON(map[string]any{
		"sessions": listLocalSessions(query, cwd, limit),
		"host":     "oracle",
	}), nil
}

func handleSession(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	id := getStringArg(args, "session_id", "")
	machine := getStringArg(args, "machine", "")
	maxMessages := getIntArg(args, "max_messages", 20)
	maxChars := getIntArg(args, "max_chars", 4000)
	host, sum, msgs, err := readSessionAnywhere(ctx, id, machine, maxMessages, maxChars)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return toolJSON(map[string]any{
		"session_id": sum.SessionID,
		"machine":    host,
		"title":      sum.Title,
		"cwd":        sum.CWD,
		"model":      sum.Model,
		"thinking":   sum.Thinking,
		"fallback":   sum.Fallback,
		"mtime":      sum.MTime,
		"messages":   msgs,
	}), nil
}
