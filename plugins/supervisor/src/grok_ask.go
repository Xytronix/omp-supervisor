package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	grokAskRetention  = 7 * 24 * time.Hour
	grokAskMaxRecords = 256

	grokAskPosting     = "posting"
	grokAskDelivered   = "delivered"
	grokAskUnconfirmed = "unconfirmed"
	grokAskFailed      = "failed"
	grokAskAnswered    = "answered"
)

func grokAskOpen(status string) bool {
	return status == grokAskPosting || status == grokAskDelivered || status == grokAskUnconfirmed
}

func grokAskState(status string) string {
	switch status {
	case grokAskAnswered:
		return "answered"
	case grokAskFailed:
		return "failed"
	}
	return "pending"
}

type grokAsk struct {
	ID          string    `json:"id"`
	Peer        string    `json:"peer"`
	From        string    `json:"from,omitempty"`
	Prompt      string    `json:"prompt"`
	TokenHash   string    `json:"token_hash"`
	Status      string    `json:"status"`
	Answer      string    `json:"answer,omitempty"`
	Error       string    `json:"error,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	DeliveredAt time.Time `json:"delivered_at"`
	AnsweredAt  time.Time `json:"answered_at"`
}

func (a *grokAsk) expiresAt() time.Time { return a.CreatedAt.Add(grokAskRetention) }

type grokAskResult struct {
	Peer      string
	Transport string
	Ask       grokAsk
}

type grokAskStore struct {
	mu     sync.Mutex
	dir    string
	asks   map[string]*grokAsk
	byHash map[string]string
	done   map[string]chan struct{}
}

var globalGrokAsks *grokAskStore

var grokWebhookClient = &http.Client{Timeout: 30 * time.Second}

var (
	errGrokAskUnknown  = errors.New("unknown or expired callback id")
	errGrokAskConflict = errors.New("callback already answered with different content")
	errGrokAskCapacity = fmt.Errorf("too many open grok asks (%d still awaiting answers); nothing was sent. Capacity frees when an open ask is answered or expires (%s after it was sent); retry later", grokAskMaxRecords, grokAskRetention)
)

func newGrokAskStore(baseDir string) (*grokAskStore, error) {
	dir := filepath.Join(baseDir, "grok-asks")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create grok-asks dir: %w", err)
	}
	s := &grokAskStore{dir: dir, asks: map[string]*grokAsk{}, byHash: map[string]string{}, done: map[string]chan struct{}{}}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(name, ".json.tmp") {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				log.Printf("Warning: failed to remove interrupted grok ask write %s: %v", name, err)
			}
			continue
		}
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			log.Printf("Warning: skipping unreadable grok ask record %s: %v", name, err)
			continue
		}
		var a grokAsk
		if err := json.Unmarshal(data, &a); err != nil {
			log.Printf("Warning: skipping corrupt grok ask record %s: %v", name, err)
			continue
		}
		if a.ID == "" || a.TokenHash == "" || a.ID+".json" != name {
			log.Printf("Warning: skipping invalid grok ask record %s (missing id/token hash or name mismatch)", name)
			continue
		}
		if other, dup := s.byHash[a.TokenHash]; dup {
			log.Printf("Warning: skipping grok ask record %s: callback token hash already used by %s", name, other)
			continue
		}
		s.asks[a.ID] = &a
		s.byHash[a.TokenHash] = a.ID
	}
	s.mu.Lock()
	s.pruneLocked(time.Now(), 0)

	for _, a := range s.asks {
		if a.Status != grokAskPosting {
			continue
		}
		updated := *a
		updated.Status = grokAskUnconfirmed
		updated.Error = "supervisor restarted before the webhook POST outcome was recorded"
		if err := s.saveLocked(&updated); err != nil {
			log.Printf("Warning: failed to save grok ask %s: %v", a.ID, err)
			continue
		}
		*a = updated
	}
	s.mu.Unlock()
	log.Printf("Loaded %d grok asks from %s", len(s.asks), dir)
	return s, nil
}

func hashCallbackToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (s *grokAskStore) saveLocked(a *grokAsk) error {
	data, err := json.MarshalIndent(a, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(s.dir, a.ID+".json")
	if err := writeFileSync(path+".tmp", data, 0600); err != nil {
		return err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}
	return syncDir(s.dir)
}

func (s *grokAskStore) deleteLocked(id string) {
	a := s.asks[id]
	if a == nil {
		return
	}
	delete(s.asks, id)
	delete(s.byHash, a.TokenHash)
	delete(s.done, id)
	if err := os.Remove(filepath.Join(s.dir, id+".json")); err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: failed to remove grok ask %s: %v", id, err)
	}
}

func (s *grokAskStore) pruneLocked(now time.Time, reserve int) error {
	for id, a := range s.asks {
		if !now.Before(a.expiresAt()) {
			s.deleteLocked(id)
		}
	}
	excess := len(s.asks) + reserve - grokAskMaxRecords
	if excess <= 0 {
		return nil
	}
	settled := make([]*grokAsk, 0, len(s.asks))
	for _, a := range s.asks {
		if !grokAskOpen(a.Status) {
			settled = append(settled, a)
		}
	}
	sort.Slice(settled, func(i, j int) bool { return settled[i].CreatedAt.Before(settled[j].CreatedAt) })
	for _, a := range settled[:min(excess, len(settled))] {
		s.deleteLocked(a.ID)
	}
	if excess > len(settled) {
		return errGrokAskCapacity
	}
	return nil
}

func (s *grokAskStore) create(peer, prompt, from string) (grokAsk, string, <-chan struct{}, error) {
	id, err := newCallbackID()
	if err != nil {
		return grokAsk{}, "", nil, err
	}
	token, err := newCallbackID()
	if err != nil {
		return grokAsk{}, "", nil, err
	}
	now := time.Now()
	a := &grokAsk{
		ID:        "ask_" + now.UTC().Format("20060102_150405") + "_" + id[:8],
		Peer:      peer,
		From:      from,
		Prompt:    prompt,
		TokenHash: hashCallbackToken(token),
		Status:    grokAskPosting,
		CreatedAt: now,
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.pruneLocked(now, 1); err != nil {
		return grokAsk{}, "", nil, err
	}
	if err := s.saveLocked(a); err != nil {
		return grokAsk{}, "", nil, fmt.Errorf("persist ask: %w", err)
	}
	done := make(chan struct{})
	s.asks[a.ID] = a
	s.byHash[a.TokenHash] = a.ID
	s.done[a.ID] = done
	return *a, token, done, nil
}

func (s *grokAskStore) settlePost(id, status, errText string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.asks[id]
	if a == nil || a.Status != grokAskPosting {
		return
	}
	updated := *a
	updated.Status = status
	updated.Error = errText
	if status == grokAskDelivered {
		updated.DeliveredAt = time.Now()
	}
	if err := s.saveLocked(&updated); err != nil {
		log.Printf("Warning: failed to save grok ask %s: %v", id, err)
		return
	}
	*a = updated
}

func (s *grokAskStore) answer(token, answer string) (ask grokAsk, duplicate bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.byHash[hashCallbackToken(token)]
	if !ok {
		return grokAsk{}, false, errGrokAskUnknown
	}
	a := s.asks[id]
	if a == nil {
		delete(s.byHash, hashCallbackToken(token))
		return grokAsk{}, false, errGrokAskUnknown
	}
	now := time.Now()
	if !now.Before(a.expiresAt()) {
		s.deleteLocked(id)
		return grokAsk{}, false, errGrokAskUnknown
	}
	if a.Status == grokAskAnswered {
		if a.Answer == answer {
			return *a, true, nil
		}
		return *a, false, errGrokAskConflict
	}
	updated := *a
	updated.Status = grokAskAnswered
	updated.Answer = answer
	updated.Error = ""
	updated.AnsweredAt = now
	if err := s.saveLocked(&updated); err != nil {
		return grokAsk{}, false, err
	}
	*a = updated
	if done := s.done[id]; done != nil {
		close(done)
		delete(s.done, id)
	}
	return *a, false, nil
}

func (s *grokAskStore) get(id string) (grokAsk, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.asks[id]
	if a == nil || !time.Now().Before(a.expiresAt()) {
		return grokAsk{}, false
	}
	return *a, true
}

func (s *grokAskStore) forPeer(peer string) []grokAsk {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var out []grokAsk
	for _, a := range s.asks {
		if a.Peer == peer && now.Before(a.expiresAt()) {
			out = append(out, *a)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID > out[j].ID
	})
	return out
}

func publicCallbackBase() string {
	base := strings.TrimSpace(os.Getenv("SUPERVISOR_PUBLIC_BASE_URL"))
	if base == "" {
		port := os.Getenv("FASTMCP_PORT")
		if port == "" {
			port = "8000"
		}
		base = "http://127.0.0.1:" + port
	}
	return strings.TrimRight(base, "/")
}

func resolveGrokPeer(ref string) (*GrokTarget, error) {
	if !isGrokPeerID(ref) {
		return nil, nil
	}
	targets, err := loadCompletionWebhookTargets()
	if err != nil {
		return nil, err
	}
	for _, target := range targets {
		if target.ID == ref {
			return &target, nil
		}
	}
	return nil, nil
}

func listGrokPeerViews() ([]agentView, error) {
	targets, err := loadCompletionWebhookTargets()
	if err != nil {
		return nil, err
	}
	out := make([]agentView, 0, len(targets))
	for _, target := range targets {
		out = append(out, agentView{
			AgentID:      target.ID,
			Source:       "grok",
			Transport:    "webhook",
			Status:       "configured",
			Name:         target.Name,
			Steerable:    false,
			Capabilities: []string{"prompt"},
		})
	}
	return out, nil
}

func newCallbackID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func projectGrokAsk(a grokAsk, maxChars int) map[string]any {
	out := map[string]any{
		"request_id": a.ID,
		"status":     a.Status,
		"state":      grokAskState(a.Status),
		"created_at": a.CreatedAt,
		"expires_at": a.expiresAt(),
	}
	prompt, answer := a.Prompt, a.Answer
	if maxChars > 0 {
		var pTrunc, aTrunc bool
		prompt, pTrunc = headCut(prompt, maxChars)
		answer, aTrunc = headCut(answer, maxChars)
		if pTrunc || aTrunc {
			out["truncated"] = true
		}
	}
	out["prompt"] = prompt
	if a.From != "" {
		out["from"] = a.From
	}
	if a.Status == grokAskAnswered {
		out["result"] = answer
		out["answered_at"] = a.AnsweredAt
	}
	if !a.DeliveredAt.IsZero() {
		out["delivered_at"] = a.DeliveredAt
	}
	if a.Error != "" {
		out["error"] = a.Error
	}
	return out
}

func buildGrokAskPayload(ask, from, callbackURL string, source *PromptSource) map[string]any {
	ask = truncateRunes(strings.TrimSpace(ask), maxAskRunes)
	p := map[string]any{
		"source":       "supervisor",
		"event":        "ask",
		"prompt":       ask,
		"message":      ask,
		"text":         ask,
		"callback_url": callbackURL,
	}
	if from != "" {
		p["from"] = from
	}

	if real := realAgentSource(source); real != nil {
		p["prompt_source"] = real
	}
	return p
}

func realAgentSource(source *PromptSource) *PromptSource {
	if source == nil || source.Kind != "agent" || strings.TrimSpace(source.From) == "" {
		return nil
	}
	copy := *source
	return &copy
}

func askGrokPeer(ctx context.Context, peer GrokTarget, text, from string, source *PromptSource, wait time.Duration) (*grokAskResult, error) {
	if wait > 5*time.Minute {
		wait = 5 * time.Minute
	}
	store := globalGrokAsks
	if store == nil {
		return nil, errors.New("grok ask store is not initialized")
	}

	prompt := truncateRunes(strings.TrimSpace(text), maxAskRunes)
	ask, token, done, err := store.create(peer.ID, prompt, from)
	if err != nil {
		if errors.Is(err, errGrokAskCapacity) {
			log.Printf("grok ask: refused new ask to %s: %d open asks at capacity, nothing sent", peer.Name, grokAskMaxRecords)
		}
		return nil, err
	}

	notSent := func(status, msg string) (*grokAskResult, error) {
		store.settlePost(ask.ID, status, msg)
		current, ok := store.get(ask.ID)
		if !ok {
			current = ask
		}
		res := &grokAskResult{Peer: peer.ID, Transport: "webhook", Ask: current}
		if current.Status == grokAskAnswered {
			return res, nil
		}
		return res, fmt.Errorf("%s (request %s)", msg, ask.ID)
	}
	callbackURL := publicCallbackBase() + "/api/agents/callback/" + token
	body, err := json.Marshal(buildGrokAskPayload(prompt, from, callbackURL, source))
	if err != nil {
		return notSent(grokAskFailed, "marshal ask: "+err.Error())
	}

	req, err := http.NewRequestWithContext(context.WithoutCancel(ctx), http.MethodPost, peer.URL, bytes.NewReader(body))
	if err != nil {
		return notSent(grokAskFailed, fmt.Sprintf("webhook request to %s: build failed", peer.Name))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+peer.Key)

	resp, err := grokWebhookClient.Do(req)
	if err != nil {

		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}

		return notSent(grokAskUnconfirmed, fmt.Sprintf("webhook post to %s did not confirm delivery: %v", peer.Name, err))
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return notSent(grokAskFailed, fmt.Sprintf("webhook %s HTTP %d: %s", peer.Name, resp.StatusCode, strings.TrimSpace(string(respBody))))
	}
	store.settlePost(ask.ID, grokAskDelivered, "")
	log.Printf("grok ask: posted %s to %s", ask.ID, peer.Name)

	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-done:
		case <-ctx.Done():
		case <-timer.C:
		}
	}

	if current, ok := store.get(ask.ID); ok {
		ask = current
	}
	return &grokAskResult{Peer: peer.ID, Transport: "webhook", Ask: ask}, nil
}

func handleGrokCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	token := r.PathValue("id")
	if token == "" {

		token = strings.TrimPrefix(r.URL.Path, "/api/agents/callback/")
		token = strings.Trim(token, "/")
	}
	if token == "" || strings.Contains(token, "/") {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "missing callback id"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "read body"})
		return
	}
	var payload map[string]any
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid json"})
			return
		}
	}
	answer := firstString(payload, "answer", "result", "text", "message", "report")
	if answer == "" {
		answer = strings.TrimSpace(string(body))
	}

	if globalGrokAsks == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "error": errGrokAskUnknown.Error()})
		return
	}
	ask, duplicate, err := globalGrokAsks.answer(token, answer)
	switch {
	case errors.Is(err, errGrokAskUnknown):
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "error": err.Error()})
	case errors.Is(err, errGrokAskConflict):
		log.Printf("grok ask: conflicting callback for %s (peer=%s) ignored; first answer kept", ask.ID, ask.Peer)
		writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": err.Error()})
	case err != nil:
		log.Printf("grok ask: failed to store answer for a callback: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"ok": false, "error": "answer not stored; retry"})
	case duplicate:
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "duplicate": true})
	default:
		log.Printf("grok ask: answer stored for %s (peer=%s) answer_len=%d", ask.ID, ask.Peer, len(answer))
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	}
}

func firstString(m map[string]any, keys ...string) string {
	if m == nil {
		return ""
	}
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch t := v.(type) {
			case string:
				if s := strings.TrimSpace(t); s != "" {
					return s
				}
			}
		}
	}
	return ""
}
