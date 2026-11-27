package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultAgentTTL      = 90 * time.Second
	minAgentTTL          = 15 * time.Second
	maxAgentTTL          = 15 * time.Minute
	agentGrace           = 10 * time.Minute
	maxInboxWait         = 60 * time.Second
	defaultInboxWait     = 25 * time.Second
	maxDeliveryTextBytes = 32 * 1024
	maxQueuedDeliveries  = 50

	maxDeliveryHistory   = 20
	maxUnackedDeliveries = 100
	defaultMaxAgents     = 200
	maxRegistryBody      = 64 * 1024
	heartbeatSaveEvery   = 30 * time.Second

	authFailLimit  = 30
	authFailWindow = 5 * time.Minute
)

type Delivery struct {
	ID          string        `json:"id"`
	Text        string        `json:"text"`
	Mode        string        `json:"mode"`
	Status      string        `json:"status"`
	From        string        `json:"from,omitempty"`
	Source      *PromptSource `json:"source,omitempty"`
	CreatedAt   time.Time     `json:"created_at"`
	DeliveredAt time.Time     `json:"delivered_at,omitempty"`
	AckedAt     time.Time     `json:"acked_at,omitempty"`
	Result      string        `json:"result,omitempty"`
	Error       string        `json:"error,omitempty"`
}

type Agent struct {
	ID           string            `json:"id"`
	Name         string            `json:"name,omitempty"`
	Machine      string            `json:"machine,omitempty"`
	Host         *hostIdentity     `json:"host,omitempty"`
	Harness      string            `json:"harness,omitempty"`
	Version      string            `json:"version,omitempty"`
	SessionID    string            `json:"session_id,omitempty"`
	CWD          string            `json:"cwd,omitempty"`
	Model        string            `json:"model,omitempty"`
	Title        string            `json:"title,omitempty"`
	PID          int               `json:"pid,omitempty"`
	Capabilities []string          `json:"capabilities,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	RemoteAddr   string            `json:"remote_addr,omitempty"`
	TokenSHA256  string            `json:"token_sha256"`
	TTLSeconds   int               `json:"ttl_seconds"`
	RegisteredAt time.Time         `json:"registered_at"`
	LastSeen     time.Time         `json:"last_seen"`
	Busy         bool              `json:"busy"`

	BusyReported bool `json:"busy_reported,omitempty"`

	BusySince       time.Time   `json:"busy_since,omitempty"`
	LastTurnEndedAt time.Time   `json:"last_turn_ended_at,omitempty"`
	Queue           []*Delivery `json:"queue,omitempty"`
	History         []*Delivery `json:"history,omitempty"`

	notify   chan struct{} `json:"-"`
	lastSave time.Time     `json:"-"`
}

func (a *Agent) status(now time.Time) string {
	if now.Sub(a.LastSeen) > a.ttl() {
		return "stale"
	}
	return "live"
}

func (a *Agent) ttl() time.Duration {
	if a.TTLSeconds <= 0 {
		return defaultAgentTTL
	}
	return time.Duration(a.TTLSeconds) * time.Second
}

type AgentRegistry struct {
	mu     sync.Mutex
	dir    string
	agents map[string]*Agent
	auth   *registryAuth
}

var globalAgents *AgentRegistry

type registryAuth struct {
	mu            sync.Mutex
	enrollHashes  []string
	allowMachines []string
	allowNames    []string
	maxAgents     int
	disabled      bool
	tokenPath     string
	generated     bool
	failures      map[string]*authFailure
}

type authFailure struct {
	count int
	since time.Time
}

func hashToken(tok string) string {
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

func splitList(raw string) []string {
	var out []string
	for _, part := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == ' ' || r == '\t'
	}) {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func newRegistryAuth(baseDir string) (*registryAuth, error) {
	a := &registryAuth{
		maxAgents: defaultMaxAgents,
		failures:  make(map[string]*authFailure),
	}
	a.disabled = truthyEnv(os.Getenv("SUPERVISOR_REGISTRY_DISABLED"))
	a.allowMachines = splitList(os.Getenv("SUPERVISOR_REGISTRY_ALLOW_MACHINES"))
	a.allowNames = splitList(os.Getenv("SUPERVISOR_REGISTRY_ALLOW_NAMES"))
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv("SUPERVISOR_REGISTRY_MAX_AGENTS"))); err == nil && v > 0 {
		a.maxAgents = v
	}

	tokens := splitList(os.Getenv("SUPERVISOR_REGISTRY_TOKEN"))
	if file := strings.TrimSpace(os.Getenv("SUPERVISOR_REGISTRY_TOKEN_FILE")); file != "" {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("SUPERVISOR_REGISTRY_TOKEN_FILE %s: %w", file, err)
		}
		tokens = append(tokens, splitList(string(data))...)
	}

	if len(tokens) == 0 {
		tokenPath := filepath.Join(baseDir, "registry-token")
		data, err := os.ReadFile(tokenPath)
		if err == nil && len(splitList(string(data))) > 0 {
			tokens = splitList(string(data))
		} else {
			generated, genErr := randomToken(32)
			if genErr != nil {
				return nil, genErr
			}
			if err := os.WriteFile(tokenPath, []byte(generated+"\n"), 0600); err != nil {
				return nil, fmt.Errorf("failed to persist generated registry token: %w", err)
			}
			tokens = []string{generated}
			a.generated = true
		}
		a.tokenPath = tokenPath
	}

	for _, tok := range tokens {
		a.enrollHashes = append(a.enrollHashes, hashToken(tok))
	}
	if len(a.enrollHashes) == 0 {
		return nil, fmt.Errorf("registry has no enrollment token")
	}
	return a, nil
}

func truthyEnv(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random token: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func (a *registryAuth) acceptsEnrollToken(tok string) bool {
	if tok == "" {
		return false
	}
	got := hashToken(tok)
	ok := false
	for _, want := range a.enrollHashes {
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1 {
			ok = true
		}
	}
	return ok
}

func (a *registryAuth) allows(machine, name string) error {
	if err := matchAllowList("machine", machine, a.allowMachines); err != nil {
		return err
	}
	return matchAllowList("name", name, a.allowNames)
}

func matchAllowList(kind, value string, patterns []string) error {
	if len(patterns) == 0 {
		return nil
	}
	for _, pattern := range patterns {
		if ok, err := path.Match(pattern, value); err == nil && ok {
			return nil
		}
	}
	return fmt.Errorf("%s %q is not in SUPERVISOR_REGISTRY_ALLOW_%sS", kind, value, strings.ToUpper(kind))
}

func (a *registryAuth) throttled(ip string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	entry, ok := a.failures[ip]
	if !ok || now.Sub(entry.since) > authFailWindow {
		entry = &authFailure{since: now}
		a.failures[ip] = entry
	}
	entry.count++

	if len(a.failures) > 1024 {
		for key, val := range a.failures {
			if now.Sub(val.since) > authFailWindow {
				delete(a.failures, key)
			}
		}
	}
	return entry.count > authFailLimit
}

func (a *registryAuth) isThrottled(ip string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	entry, ok := a.failures[ip]
	if !ok || time.Since(entry.since) > authFailWindow {
		return false
	}
	return entry.count > authFailLimit
}

func newAgentRegistry(baseDir string) (*AgentRegistry, error) {
	dir := filepath.Join(baseDir, "agents")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create agents dir: %w", err)
	}
	auth, err := newRegistryAuth(baseDir)
	if err != nil {
		return nil, err
	}
	r := &AgentRegistry{dir: dir, agents: make(map[string]*Agent), auth: auth}

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
		var ag Agent
		if err := json.Unmarshal(data, &ag); err != nil || ag.ID == "" {
			continue
		}
		ag.notify = make(chan struct{}, 1)
		ag.History = trimDeliveryHistory(ag.History)
		r.agents[ag.ID] = &ag
	}
	log.Printf("Loaded %d agent registrations from %s", len(r.agents), dir)
	return r, nil
}

func generateAgentID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("agt_%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("agt_%s_%s", time.Now().Format("20060102_150405"), hex.EncodeToString(b))
}

func generateDeliveryID() string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("dlv_%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("dlv_%s", hex.EncodeToString(b))
}

func (r *AgentRegistry) saveLocked(ag *Agent) {
	data, err := json.MarshalIndent(ag, "", "  ")
	if err != nil {
		log.Printf("Warning: failed to marshal agent %s: %v", ag.ID, err)
		return
	}
	if err := os.WriteFile(filepath.Join(r.dir, ag.ID+".json"), data, 0600); err != nil {
		log.Printf("Warning: failed to save agent %s: %v", ag.ID, err)
		return
	}
	ag.lastSave = time.Now()
}

func (r *AgentRegistry) deleteLocked(ag *Agent) {
	delete(r.agents, ag.ID)
	if err := os.Remove(filepath.Join(r.dir, ag.ID+".json")); err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: failed to remove agent file %s: %v", ag.ID, err)
	}
}

type registration struct {
	AgentID      string            `json:"agent_id"`
	Name         string            `json:"name"`
	Machine      string            `json:"machine"`
	Host         *hostIdentity     `json:"host,omitempty"`
	Harness      string            `json:"harness"`
	Version      string            `json:"version"`
	SessionID    string            `json:"session_id"`
	CWD          string            `json:"cwd"`
	Model        string            `json:"model"`
	Title        string            `json:"title"`
	PID          int               `json:"pid"`
	Capabilities []string          `json:"capabilities"`
	Labels       map[string]string `json:"labels"`
	TTLSeconds   int               `json:"ttl_seconds"`
}

type heartbeat struct {
	Host       *hostIdentity `json:"host,omitempty"`
	SessionID  *string       `json:"session_id"`
	CWD        string        `json:"cwd"`
	Model      *string       `json:"model"`
	Title      *string       `json:"title"`
	TTLSeconds int           `json:"ttl_seconds"`
	Busy       *bool         `json:"busy"`

	TurnEndedAt *time.Time `json:"turn_ended_at,omitempty"`
}

func clampTTL(seconds int) time.Duration {
	ttl := time.Duration(seconds) * time.Second
	if seconds <= 0 {
		ttl = defaultAgentTTL
	}
	if ttl < minAgentTTL {
		ttl = minAgentTTL
	}
	if ttl > maxAgentTTL {
		ttl = maxAgentTTL
	}
	return ttl
}

var errRegistrationConflict = errors.New("registration conflict")

func (r *AgentRegistry) register(reg registration, remoteAddr string) (*Agent, string, error) {
	if err := r.auth.allows(reg.Machine, reg.Name); err != nil {
		return nil, "", err
	}
	token, err := randomToken(32)
	if err != nil {
		return nil, "", err
	}
	ttl := clampTTL(reg.TTLSeconds)
	now := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()
	var existing *Agent
	resumed := false

	if reg.AgentID != "" {
		existing = r.agents[reg.AgentID]
		resumed = existing != nil
	}
	if existing == nil && reg.SessionID != "" {
		for _, ag := range r.agents {
			if ag.SessionID == reg.SessionID && ag.Machine == reg.Machine {
				existing = ag
				break
			}
		}
	}

	if existing != nil && !resumed && existing.status(now) == "live" &&
		reg.Harness != "" && existing.Harness != "" && reg.Harness != existing.Harness {
		return nil, "", fmt.Errorf("%w: session %s on %s is live as %s (%s); stop it or revoke %s first",
			errRegistrationConflict, reg.SessionID, reg.Machine, existing.ID, existing.Harness, existing.ID)
	}

	if existing == nil && len(r.agents) >= r.auth.maxAgents {

		var oldest *Agent
		for _, ag := range r.agents {
			if ag.status(now) == "stale" && (oldest == nil || ag.LastSeen.Before(oldest.LastSeen)) {
				oldest = ag
			}
		}
		if oldest == nil {
			return nil, "", fmt.Errorf("registry is full (%d agents); raise SUPERVISOR_REGISTRY_MAX_AGENTS", r.auth.maxAgents)
		}
		r.deleteLocked(oldest)
	}

	ag := existing
	if ag == nil {
		ag = &Agent{ID: generateAgentID(), RegisteredAt: now, notify: make(chan struct{}, 1)}
		r.agents[ag.ID] = ag
	}
	if ag.notify == nil {
		ag.notify = make(chan struct{}, 1)
	}
	ag.Name = reg.Name
	ag.Machine = reg.Machine
	ag.Host = normalizedHostIdentity(reg.Host)
	ag.Harness = reg.Harness
	ag.Version = reg.Version
	ag.SessionID = reg.SessionID
	ag.CWD = reg.CWD
	ag.Model = reg.Model
	ag.Title = reg.Title
	ag.PID = reg.PID
	ag.Capabilities = reg.Capabilities
	ag.Labels = reg.Labels
	ag.RemoteAddr = remoteAddr
	ag.TTLSeconds = int(ttl / time.Second)
	ag.LastSeen = now
	ag.TokenSHA256 = hashToken(token)
	r.saveLocked(ag)

	return ag.snapshotLocked(), token, nil
}

func (r *AgentRegistry) authenticate(id, token string) (*Agent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ag, ok := r.agents[id]
	if !ok {
		return nil, errUnknownAgent
	}
	if token == "" || subtle.ConstantTimeCompare([]byte(hashToken(token)), []byte(ag.TokenSHA256)) != 1 {
		return nil, errBadAgentToken
	}
	return ag, nil
}

var (
	errUnknownAgent  = fmt.Errorf("unknown agent")
	errBadAgentToken = fmt.Errorf("invalid agent token")
)

func (r *AgentRegistry) touch(ag *Agent, update *heartbeat) Agent {
	r.mu.Lock()
	defer r.mu.Unlock()
	ag.LastSeen = time.Now()
	if update.Host != nil {
		ag.Host = normalizedHostIdentity(update.Host)
	}
	if update.Model != nil {
		ag.Model = *update.Model
	}
	if update.Title != nil {
		ag.Title = *update.Title
	}
	if update.CWD != "" {
		ag.CWD = update.CWD
	}
	if update.SessionID != nil {
		ag.SessionID = *update.SessionID
	}
	if update.TTLSeconds > 0 {
		ag.TTLSeconds = int(clampTTL(update.TTLSeconds) / time.Second)
	}
	if update.Busy != nil {
		r.observeBusyLocked(ag, *update.Busy, update.TurnEndedAt)
	}
	if time.Since(ag.lastSave) > heartbeatSaveEvery {
		r.saveLocked(ag)
	}
	return *ag
}

func (r *AgentRegistry) observeBusyLocked(ag *Agent, busy bool, turnEndedAt *time.Time) {
	now := time.Now()
	changed := !ag.BusyReported || ag.Busy != busy
	if busy && (changed || ag.BusySince.IsZero()) {
		ag.BusySince = now
	}
	if !busy {
		ended := ag.LastTurnEndedAt
		if turnEndedAt != nil && !turnEndedAt.IsZero() {

			ended = turnEndedAt.UTC()
			if ended.After(now) {
				ended = now
			}
		} else if ag.BusyReported && ag.Busy {

			ended = now
		}
		if !ended.Equal(ag.LastTurnEndedAt) {
			ag.LastTurnEndedAt = ended
			changed = true
		}
	}
	ag.Busy = busy
	ag.BusyReported = true
	if changed {
		r.saveLocked(ag)
	}
}

func (a *Agent) turnState(now time.Time) string {
	if !a.BusyReported || a.status(now) != "live" {
		return "unknown"
	}
	if a.Busy {
		return "busy"
	}
	return "idle"
}

func (r *AgentRegistry) enqueue(id, text, mode, from string, source *PromptSource) (*Delivery, error) {
	if len(text) > maxDeliveryTextBytes {
		return nil, fmt.Errorf("text is %d bytes; limit is %d", len(text), maxDeliveryTextBytes)
	}
	r.mu.Lock()
	ag, ok := r.agents[id]
	if !ok {
		r.mu.Unlock()
		return nil, errUnknownAgent
	}
	if mode == "aside" && !slices.Contains(ag.Capabilities, "aside") {
		r.mu.Unlock()
		return nil, fmt.Errorf("agent %s lacks the aside capability; restart this session with the updated Supervisor bridge before sending aside input", id)
	}
	if len(ag.Queue) >= maxQueuedDeliveries {
		r.mu.Unlock()
		return nil, fmt.Errorf("agent %s already has %d queued prompts", id, len(ag.Queue))
	}
	d := &Delivery{
		ID:        generateDeliveryID(),
		Text:      text,
		Mode:      mode,
		Status:    "queued",
		From:      from,
		Source:    source,
		CreatedAt: time.Now(),
	}
	ag.Queue = append(ag.Queue, d)
	r.saveLocked(ag)
	notify := ag.notify

	queued := *d
	r.mu.Unlock()

	select {
	case notify <- struct{}{}:
	default:
	}
	return &queued, nil
}

func (r *AgentRegistry) claimQueue(ag *Agent) []*Delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(ag.Queue) == 0 {
		ag.LastSeen = time.Now()
		return nil
	}
	now := time.Now()
	out := make([]*Delivery, 0, len(ag.Queue))
	for _, d := range ag.Queue {
		d.Status = "delivered"
		d.DeliveredAt = now
		copied := *d
		out = append(out, &copied)
		ag.History = append(ag.History, d)
	}
	ag.Queue = nil
	ag.History = trimDeliveryHistory(ag.History)
	ag.LastSeen = now
	r.saveLocked(ag)
	return out
}

func (r *AgentRegistry) ack(ag *Agent, deliveryID, status, result, errText string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, d := range ag.History {
		if d.ID != deliveryID {
			continue
		}
		d.AckedAt = time.Now()
		d.Result = result
		d.Error = errText
		if status == "error" || errText != "" {
			d.Status = "failed"
		} else {
			d.Status = "acked"
		}
		ag.LastSeen = time.Now()
		ag.History = trimDeliveryHistory(ag.History)
		r.saveLocked(ag)
		return nil
	}
	return fmt.Errorf("delivery %q not found for agent %s", deliveryID, ag.ID)
}

func trimDeliveryHistory(history []*Delivery) []*Delivery {
	var settled []*Delivery
	unacked := 0
	for _, d := range history {
		if d.Status == "delivered" {
			unacked++
		} else {
			settled = append(settled, d)
		}
	}
	if len(settled) <= maxDeliveryHistory && unacked <= maxUnackedDeliveries {
		return history
	}
	drop := make(map[*Delivery]bool)
	if excess := len(settled) - maxDeliveryHistory; excess > 0 {
		slices.SortStableFunc(settled, func(a, b *Delivery) int { return a.AckedAt.Compare(b.AckedAt) })
		for _, d := range settled[:excess] {
			drop[d] = true
		}
	}
	dropUnacked := max(unacked-maxUnackedDeliveries, 0)
	kept := make([]*Delivery, 0, len(history)-len(drop)-dropUnacked)
	for _, d := range history {
		if d.Status == "delivered" && dropUnacked > 0 {
			dropUnacked--
			continue
		}
		if drop[d] {
			continue
		}
		kept = append(kept, d)
	}
	return kept
}

func (r *AgentRegistry) findDelivery(agentID, deliveryID string) *Delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	ag, ok := r.agents[agentID]
	if !ok {
		return nil
	}
	for _, d := range append(append([]*Delivery{}, ag.Queue...), ag.History...) {
		if d.ID == deliveryID {
			copied := *d
			return &copied
		}
	}
	return nil
}

func (ag *Agent) snapshotLocked() *Agent {
	copied := *ag
	if ag.Host != nil {
		host := *ag.Host
		copied.Host = &host
	}
	copied.Queue = cloneDeliveries(ag.Queue)
	copied.History = cloneDeliveries(ag.History)
	return &copied
}

func cloneDeliveries(in []*Delivery) []*Delivery {
	if len(in) == 0 {
		return nil
	}
	out := make([]*Delivery, 0, len(in))
	for _, d := range in {
		copied := *d
		out = append(out, &copied)
	}
	return out
}

func (r *AgentRegistry) get(id string) *Agent {
	r.mu.Lock()
	defer r.mu.Unlock()
	ag, ok := r.agents[id]
	if !ok {
		return nil
	}
	return ag.snapshotLocked()
}

func (r *AgentRegistry) resolve(ref string, allowAliases bool) *Agent {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil
	}
	if ag := r.get(ref); ag != nil {
		return ag
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var matches []*Agent
	for _, ag := range r.agents {
		if ag.SessionID == ref || allowAliases && (ag.Name == ref || strings.HasPrefix(ag.ID, ref)) {
			matches = append(matches, ag)
		}
	}
	if len(matches) != 1 {
		return nil
	}
	return matches[0].snapshotLocked()
}

func (r *AgentRegistry) list() []*Agent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Agent, 0, len(r.agents))
	for _, ag := range r.agents {
		out = append(out, ag.snapshotLocked())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].LastSeen.After(out[j].LastSeen) })
	return out
}

func (r *AgentRegistry) remove(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	ag, ok := r.agents[id]
	if !ok {
		return false
	}
	r.deleteLocked(ag)
	return true
}

func (r *AgentRegistry) reap() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	removed := 0
	for _, ag := range r.agents {
		if now.Sub(ag.LastSeen) > ag.ttl()+agentGrace {
			r.deleteLocked(ag)
			removed++
		}
	}
	return removed
}

func (r *AgentRegistry) startReaper(interval time.Duration) {
	go func() {
		for {
			time.Sleep(interval)
			if n := r.reap(); n > 0 {
				log.Printf("registry: reaped %d expired agent registration(s)", n)
			}
		}
	}()
}

func bearerToken(r *http.Request) string {
	header := r.Header.Get("Authorization")
	if strings.HasPrefix(strings.ToLower(header), "bearer ") {
		return strings.TrimSpace(header[7:])
	}
	if tok := r.Header.Get("X-Supervisor-Token"); tok != "" {
		return strings.TrimSpace(tok)
	}
	return ""
}

func clientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func observedAddr(r *http.Request) string {
	addr := clientIP(r)
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		first := strings.TrimSpace(strings.Split(fwd, ",")[0])
		if first != "" && first != addr {
			addr = first + " via " + addr
		}
	}
	return addr
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeRegistryError(w http.ResponseWriter, code int, errCode, msg string) {
	writeJSON(w, code, map[string]any{"error": msg, "code": errCode})
}

func decodeBody(w http.ResponseWriter, r *http.Request, into any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRegistryBody)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(into); err != nil && err.Error() != "EOF" {
		writeRegistryError(w, http.StatusBadRequest, "bad_request", fmt.Sprintf("invalid JSON body: %v", err))
		return false
	}
	return true
}

func (r *AgentRegistry) registryMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/agents/register", r.handleHTTPRegister)
	mux.HandleFunc("POST /api/agents/{id}/heartbeat", r.handleHTTPHeartbeat)
	mux.HandleFunc("GET /api/agents/{id}/inbox", r.handleHTTPInbox)
	mux.HandleFunc("POST /api/agents/{id}/ack", r.handleHTTPAck)
	mux.HandleFunc("POST /api/agents/{id}/unregister", r.handleHTTPUnregister)
	mux.HandleFunc("DELETE /api/agents/{id}", r.handleHTTPUnregister)
	return mux
}

func (r *AgentRegistry) guard(w http.ResponseWriter, req *http.Request) bool {
	if r.auth.disabled {
		writeRegistryError(w, http.StatusServiceUnavailable, "registry_disabled", "agent registry is disabled (SUPERVISOR_REGISTRY_DISABLED)")
		return false
	}
	if r.auth.isThrottled(clientIP(req)) {
		writeRegistryError(w, http.StatusTooManyRequests, "throttled", "too many failed authentication attempts; try again later")
		return false
	}
	return true
}

func (r *AgentRegistry) rejectAuth(w http.ResponseWriter, req *http.Request, errCode, msg string, code int) {
	ip := clientIP(req)
	if r.auth.throttled(ip) {
		log.Printf("registry: throttling %s after repeated auth failures", ip)
	}
	log.Printf("registry: rejected %s %s from %s (%s)", req.Method, req.URL.Path, ip, errCode)
	writeRegistryError(w, code, errCode, msg)
}

func (r *AgentRegistry) handleHTTPRegister(w http.ResponseWriter, req *http.Request) {
	if !r.guard(w, req) {
		return
	}
	if !r.auth.acceptsEnrollToken(bearerToken(req)) {
		r.rejectAuth(w, req, "unauthorized", "a valid enrollment token is required (Authorization: Bearer <token>)", http.StatusUnauthorized)
		return
	}
	var reg registration
	if !decodeBody(w, req, &reg) {
		return
	}
	ag, token, err := r.register(reg, observedAddr(req))
	if err != nil {
		if errors.Is(err, errRegistrationConflict) {
			writeRegistryError(w, http.StatusConflict, "conflict", err.Error())
			return
		}
		writeRegistryError(w, http.StatusForbidden, "rejected", err.Error())
		return
	}
	log.Printf("registry: agent %s registered (machine=%q name=%q session=%s from %s)", ag.ID, ag.Machine, ag.Name, ag.SessionID, ag.RemoteAddr)
	writeJSON(w, http.StatusOK, map[string]any{
		"agent_id":                   ag.ID,
		"agent_token":                token,
		"ttl_seconds":                ag.TTLSeconds,
		"heartbeat_interval_seconds": heartbeatInterval(ag.TTLSeconds),
		"inbox_url":                  fmt.Sprintf("/api/agents/%s/inbox", ag.ID),
		"heartbeat_url":              fmt.Sprintf("/api/agents/%s/heartbeat", ag.ID),
		"ack_url":                    fmt.Sprintf("/api/agents/%s/ack", ag.ID),
		"unregister_url":             fmt.Sprintf("/api/agents/%s/unregister", ag.ID),
		"max_inbox_wait_seconds":     int(maxInboxWait / time.Second),
		"max_delivery_text_bytes":    maxDeliveryTextBytes,
		"supported_delivery_modes":   []string{"steer", "aside", "follow_up"},
	})
}

func heartbeatInterval(ttlSeconds int) int {
	if ttlSeconds <= 0 {
		ttlSeconds = int(defaultAgentTTL / time.Second)
	}
	interval := ttlSeconds / 3
	if interval < 5 {
		interval = 5
	}
	return interval
}

func (r *AgentRegistry) authed(w http.ResponseWriter, req *http.Request) *Agent {
	if !r.guard(w, req) {
		return nil
	}
	id := req.PathValue("id")
	ag, err := r.authenticate(id, bearerToken(req))
	if err != nil {
		switch err {
		case errUnknownAgent:

			r.rejectAuth(w, req, "unknown_agent", fmt.Sprintf("agent %q is not registered; register again", id), http.StatusNotFound)
		default:
			r.rejectAuth(w, req, "unauthorized", "invalid agent token", http.StatusUnauthorized)
		}
		return nil
	}
	return ag
}

func (r *AgentRegistry) handleHTTPHeartbeat(w http.ResponseWriter, req *http.Request) {
	ag := r.authed(w, req)
	if ag == nil {
		return
	}
	var body heartbeat
	if !decodeBody(w, req, &body) {
		return
	}
	snapshot := r.touch(ag, &body)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                         true,
		"agent_id":                   snapshot.ID,
		"queued":                     len(snapshot.Queue),
		"ttl_seconds":                snapshot.TTLSeconds,
		"heartbeat_interval_seconds": heartbeatInterval(snapshot.TTLSeconds),
	})
}

func (r *AgentRegistry) handleHTTPInbox(w http.ResponseWriter, req *http.Request) {
	ag := r.authed(w, req)
	if ag == nil {
		return
	}
	wait := defaultInboxWait
	if raw := req.URL.Query().Get("wait"); raw != "" {
		if secs, err := strconv.Atoi(raw); err == nil {
			wait = time.Duration(secs) * time.Second
		}
	}
	if wait < 0 {
		wait = 0
	}
	if wait > maxInboxWait {
		wait = maxInboxWait
	}

	if items := r.claimQueue(ag); len(items) > 0 {
		writeJSON(w, http.StatusOK, map[string]any{"agent_id": ag.ID, "deliveries": items})
		return
	}
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ag.notify:
			if items := r.claimQueue(ag); len(items) > 0 {
				writeJSON(w, http.StatusOK, map[string]any{"agent_id": ag.ID, "deliveries": items})
				return
			}
		case <-timer.C:
		case <-req.Context().Done():
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"agent_id": ag.ID, "deliveries": []*Delivery{}})
}

func (r *AgentRegistry) handleHTTPAck(w http.ResponseWriter, req *http.Request) {
	ag := r.authed(w, req)
	if ag == nil {
		return
	}
	var body struct {
		DeliveryID string `json:"delivery_id"`
		Status     string `json:"status"`
		Result     string `json:"result"`
		Error      string `json:"error"`
	}
	if !decodeBody(w, req, &body) {
		return
	}
	if body.DeliveryID == "" {
		writeRegistryError(w, http.StatusBadRequest, "bad_request", "delivery_id is required")
		return
	}
	if len(body.Result) > maxDeliveryTextBytes {
		body.Result = body.Result[:maxDeliveryTextBytes]
	}
	if err := r.ack(ag, body.DeliveryID, body.Status, body.Result, body.Error); err != nil {
		writeRegistryError(w, http.StatusNotFound, "unknown_delivery", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (r *AgentRegistry) handleHTTPUnregister(w http.ResponseWriter, req *http.Request) {
	ag := r.authed(w, req)
	if ag == nil {
		return
	}
	r.remove(ag.ID)
	log.Printf("registry: agent %s unregistered", ag.ID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "agent_id": ag.ID})
}
