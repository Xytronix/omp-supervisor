package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

type PromptSource struct {
	Kind  string `json:"kind"`
	From  string `json:"from"`
	Name  string `json:"name,omitempty"`
	Title string `json:"title,omitempty"`
	Route string `json:"route,omitempty"`
}

type Job struct {
	Machine            string `json:"machine,omitempty"`
	mu                 sync.RWMutex
	saveMu             sync.Mutex
	ID                 string `json:"id"`
	PID                int    `json:"pid"`
	SessionID          string `json:"session_id"`
	ParentSessionID    string `json:"parent_session_id,omitempty"`
	RootSessionID      string `json:"root_session_id,omitempty"`
	ParentAgentID      string `json:"parent_agent_id,omitempty"`
	NotifyOnCompletion bool   `json:"notify_on_completion,omitempty"`
	ParentToolCallID   string `json:"parent_tool_call_id,omitempty"`
	MetadataVersion    int    `json:"metadata_version,omitempty"`
	Model              string `json:"model"`
	ModelRequested     string `json:"model_requested"`
	ModelResolved      string `json:"model_resolved,omitempty"`

	Fallback bool `json:"fallback"`

	FallbackFrom   string `json:"fallback_from,omitempty"`
	FallbackReason string `json:"fallback_reason,omitempty"`

	ModelsUsed []string `json:"models_used,omitempty"`

	ModelSwitches   []modelSwitch `json:"model_switches,omitempty"`
	ModelReportSeen bool          `json:"model_report_seen,omitempty"`
	Thinking        string        `json:"thinking,omitempty"`

	ThinkingRequested string `json:"thinking_requested,omitempty"`

	PolicyViolations         []string      `json:"policy_violations,omitempty"`
	Prompt                   string        `json:"prompt"`
	Source                   *PromptSource `json:"source,omitempty"`
	Title                    string        `json:"title"`
	SessionTitle             string        `json:"session_title,omitempty"`
	CWD                      string        `json:"cwd"`
	Yolo                     bool          `json:"yolo"`
	MaxTime                  string        `json:"max_time"`
	Deadline                 time.Time     `json:"deadline,omitempty"`
	LogPath                  string        `json:"log_path"`
	Status                   string        `json:"status"`
	ExitCode                 int           `json:"exit_code"`
	StartTime                time.Time     `json:"start_time"`
	EndTime                  time.Time     `json:"end_time"`
	LastTool                 string        `json:"last_tool"`
	TodosDone                int           `json:"todos_done"`
	TodosOpen                int           `json:"todos_open"`
	FilesChanged             []string      `json:"files_changed"`
	Report                   string        `json:"report"`
	Steerable                bool          `json:"steerable"`
	SessionMetadataFinalized bool          `json:"session_metadata_finalized,omitempty"`
	CaptureError             string        `json:"capture_error,omitempty"`

	CaptureAcknowledged bool        `json:"capture_acknowledged,omitempty"`
	CaptureAck          *CaptureAck `json:"capture_ack,omitempty"`

	CaptureArchive *CaptureArchive `json:"capture_archive,omitempty"`

	ContinuedFromJobID string `json:"continued_from_job_id,omitempty"`
	ContinuedByJobID   string `json:"continued_by_job_id,omitempty"`

	ackDurable bool `json:"-"`

	archiveMu        sync.Mutex `json:"-"`
	ackMu            sync.Mutex `json:"-"`
	archiveScheduled bool       `json:"-"`

	cmd             *exec.Cmd          `json:"-"`
	cancelFn        context.CancelFunc `json:"-"`
	rpc             *rpcChild          `json:"-"`
	captureDone     chan struct{}      `json:"-"`
	monitorDone     chan struct{}      `json:"-"`
	deadlineTimer   *time.Timer        `json:"-"`
	deadlineMessage string             `json:"-"`
	recovered       bool               `json:"-"`

	turnObserved       bool      `json:"-"`
	turnBusy           bool      `json:"-"`
	turnBusySince      time.Time `json:"-"`
	lastTurnEndedAt    time.Time `json:"-"`
	completionNotified bool

	needsMetadataReplay bool `json:"-"`
}

type JobStore struct {
	mu      sync.RWMutex
	jobs    map[string]*Job
	dataDir string
	logsDir string
}

const (
	defaultModelSelector = "@default"

	metadataVersion = 3

	defaultMaxChars       = 4000
	hardMaxChars          = 8000
	previewMaxChars       = 500
	promptPreviewMaxChars = 200
	agentPreviewLimit     = 3
)

var (
	globalStore *JobStore

	knownThinkingLevels = map[string]bool{
		"off":     true,
		"minimal": true,
		"low":     true,
		"medium":  true,
		"high":    true,
		"xhigh":   true,
		"max":     true,
		"auto":    true,
	}

	bearerRegex = regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/-]+=*`)
	jwtRegex    = regexp.MustCompile(`\bey[A-Za-z0-9_-]{15,}\.ey[A-Za-z0-9_-]{15,}\.[A-Za-z0-9_-]+\b`)
	apiKeyRegex = regexp.MustCompile(`\b(sk-[A-Za-z0-9_-]{20,}|ghp_[A-Za-z0-9]{30,}|AIza[A-Za-z0-9_-]{30,}|xai-[A-Za-z0-9_-]{20,}|fk-[A-Za-z0-9_-]{20,})\b`)
	authHeader  = regexp.MustCompile(`(?i)(authorization:\s*)[^\r\n]+`)
	passParam   = regexp.MustCompile(`(?i)(password|token|secret|key|api_key|auth)=([^\s&"']+)`)

	editPathRegex = regexp.MustCompile(`\[([^#\n\r\]]+)#[0-9a-fA-F]+\]`)
	todoOverallRe = regexp.MustCompile(`Overall:\s*(\d+)/(\d+)\s+done,\s*(\d+)\s+open`)
	todoRemainRe  = regexp.MustCompile(`Remaining items\s*\((\d+)\)`)
)

func newJobStore(baseDir string) (*JobStore, error) {
	dataDir := filepath.Join(baseDir, "jobs")
	logsDir := filepath.Join(baseDir, "logs")

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data dir: %w", err)
	}
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs dir: %w", err)
	}

	store := &JobStore{
		jobs:    make(map[string]*Job),
		dataDir: dataDir,
		logsDir: logsDir,
	}

	store.loadAll()
	return store, nil
}

func (s *JobStore) add(j *Job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[j.ID] = j
	s.save(j)
}

func (s *JobStore) get(id string) *Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.jobs[id]
}

func (s *JobStore) findRunningJobBySessionID(sessionID string) *Job {
	if sessionID == "" {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, j := range s.jobs {
		j.mu.RLock()
		if j.SessionID == sessionID && j.Status == "running" && isProcessAlive(j.PID) {
			j.mu.RUnlock()
			return j
		}
		j.mu.RUnlock()
	}
	return nil
}

func (s *JobStore) runningSteerableJobs() []*Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Job
	for _, j := range s.jobs {
		j.mu.RLock()
		live := j.Steerable && j.Status == "running" && isProcessAlive(j.PID)
		j.mu.RUnlock()
		if live {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(i, k int) bool {
		out[i].mu.RLock()
		a := out[i].StartTime
		out[i].mu.RUnlock()
		out[k].mu.RLock()
		b := out[k].StartTime
		out[k].mu.RUnlock()
		return a.After(b)
	})
	return out
}

func (s *JobStore) save(j *Job) error {
	if persistJobHook != nil {
		if err := persistJobHook(j); err != nil {
			log.Printf("Warning: refused to persist job %s: %v", j.ID, err)
			return err
		}
	}
	j.saveMu.Lock()
	defer j.saveMu.Unlock()
	j.mu.RLock()
	defer j.mu.RUnlock()

	filePath := filepath.Join(s.dataDir, fmt.Sprintf("%s.json", j.ID))
	data, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		log.Printf("Warning: failed to marshal job %s: %v", j.ID, err)
		return err
	}

	tmpPath := filePath + ".tmp"
	if err := writeFileSync(tmpPath, data, 0644); err != nil {
		log.Printf("Warning: failed to save job file %s: %v", filePath, err)
		return err
	}
	if err := os.Rename(tmpPath, filePath); err != nil {
		log.Printf("Warning: failed to save job file %s: %v", filePath, err)
		return err
	}
	return syncDir(s.dataDir)
}

func writeFileSync(p string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *JobStore) loadAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		log.Printf("Warning: could not read jobs data directory %s: %v", s.dataDir, err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		filePath := filepath.Join(s.dataDir, entry.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}
		var j Job
		if err := json.Unmarshal(data, &j); err != nil {
			continue
		}

		if j.ModelRequested == "" {
			j.ModelRequested = j.Model
		}
		if j.MetadataVersion < metadataVersion {

			if j.MetadataVersion == 0 {
				j.ModelResolved = ""
				j.Thinking = ""
				j.Fallback = false
			}

			j.needsMetadataReplay = j.Steerable && j.LogPath != ""
			j.MetadataVersion = metadataVersion
		}
		if j.Machine == "" || j.Machine == "supervisor-local" {
			j.Machine = localMachineName()
		}
		j.ackDurable = j.CaptureAcknowledged && j.CaptureAck != nil
		j.recovered = true
		s.reconcileRecovered(&j)
		s.jobs[j.ID] = &j
		s.armDeadlineLocked(&j)
	}
	log.Printf("Loaded %d jobs from %s", len(s.jobs), s.dataDir)
}

func (s *JobStore) ensureRecoveredMetadata(j *Job) {
	j.mu.Lock()
	if !j.needsMetadataReplay {
		j.mu.Unlock()
		return
	}
	j.needsMetadataReplay = false
	j.recoverRPCMetadata()
	j.mu.Unlock()
	s.save(j)
}

func (s *JobStore) reconcileRecovered(j *Job) {

	j.mu.Lock()
	if !j.recovered || j.Status != "running" || j.cmd != nil || j.rpc != nil || isProcessAlive(j.PID) {
		j.mu.Unlock()
		return
	}
	j.Status = "error"
	j.ExitCode = -1
	if j.EndTime.IsZero() {
		j.EndTime = time.Now()
	}
	j.mu.Unlock()
	s.save(j)
}

func generateJobID() string {
	bytes := make([]byte, 4)
	if _, err := rand.Read(bytes); err != nil {
		return fmt.Sprintf("sub_%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("sub_%s_%s", time.Now().Format("20060102_150405"), hex.EncodeToString(bytes))
}

func locateOmpBinary() (string, error) {
	candidates := []string{
		os.Getenv("OMP_BIN"),
		os.Getenv("OMP_PATH"),
		filepath.Join(homeDir(), ".local", "bin", "omp"),
	}
	if p, err := exec.LookPath("omp"); err == nil {
		candidates = append(candidates, p)
	}
	candidates = append(candidates, "/usr/local/bin/omp", "/usr/bin/omp")
	for _, p := range candidates {
		if p == "" {
			continue
		}
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p, nil
		}
	}
	return "", fmt.Errorf("omp binary not found on PATH (set OMP_BIN)")
}

var ompParentFlagSupport sync.Map

var ompParentFlagProbe = func(bin string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, _ := exec.CommandContext(ctx, bin, "--parent-session-id", "omp:probe", "--version").CombinedOutput()
	return strings.Contains(string(out), "--parent-session-id requires")
}

func ompSupportsParentSessionID(bin string) bool {
	st, err := os.Stat(bin)
	if err != nil {
		return false
	}
	key := fmt.Sprintf("%s|%d|%d", bin, st.Size(), st.ModTime().UnixNano())
	if v, ok := ompParentFlagSupport.Load(key); ok {
		return v.(bool)
	}
	supported := ompParentFlagProbe(bin)
	ompParentFlagSupport.Store(key, supported)
	if !supported {
		log.Printf("omp at %s does not accept --parent-session-id; launching without session lineage", bin)
	}
	return supported
}

func resolveJobCwd(raw string) (string, error) {
	cwd := raw
	if cwd == "" {
		cwd = homeDir()
	}
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return "", fmt.Errorf("cwd %q: %w", cwd, err)
	}
	st, err := os.Stat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("cwd %s does not exist", abs)
		}
		return "", fmt.Errorf("cwd %s: %w", abs, err)
	}
	if !st.IsDir() {
		return "", fmt.Errorf("cwd %s is not a directory", abs)
	}
	return abs, nil
}

func sanitizeSecretString(s string) string {
	if s == "" {
		return ""
	}
	res := bearerRegex.ReplaceAllString(s, "Bearer [REDACTED]")
	res = jwtRegex.ReplaceAllString(res, "[REDACTED_JWT]")
	res = apiKeyRegex.ReplaceAllString(res, "[REDACTED_KEY]")
	res = authHeader.ReplaceAllString(res, "${1}[REDACTED]")
	res = passParam.ReplaceAllString(res, "${1}=[REDACTED]")
	return res
}

func clampMaxChars(n int) int {
	if n <= 0 {
		return defaultMaxChars
	}
	if n > hardMaxChars {
		return hardMaxChars
	}
	return n
}

func headCut(s string, max int) (string, bool) {
	if max <= 0 || len(s) <= max {
		return s, false
	}
	for max > 0 && s[max]&0xc0 == 0x80 {
		max--
	}
	return s[:max], true
}

func tailCut(s string, max int) (string, bool) {
	if max <= 0 || len(s) <= max {
		return s, false
	}
	start := len(s) - max
	for start < len(s) && s[start]&0xc0 == 0x80 {
		start++
	}
	return s[start:], true
}

func extractTitle(prompt string) string {
	clean := strings.TrimSpace(prompt)
	lines := strings.Split(clean, "\n")
	firstLine := strings.TrimSpace(lines[0])
	firstLine = strings.TrimPrefix(firstLine, "# ")
	firstLine = strings.TrimPrefix(firstLine, "## ")
	firstLine = strings.TrimPrefix(firstLine, "Task: ")
	if len(firstLine) > 60 {
		return firstLine[:57] + "..."
	}
	if firstLine != "" {
		return firstLine
	}
	return "OMP Subagent Task"
}

func parseModelAndThinking(rawModel, thinkingArg string) (modelName string, thinking string, isThinkingSplit bool) {
	if rawModel == "" {
		return "", thinkingArg, false
	}
	modelName = rawModel
	thinking = thinkingArg
	if idx := strings.LastIndex(rawModel, ":"); idx != -1 {
		suffix := rawModel[idx+1:]
		if knownThinkingLevels[suffix] {
			modelName = rawModel[:idx]
			isThinkingSplit = true
			if thinking == "" {
				thinking = suffix
			}
		}
	}
	return modelName, thinking, isThinkingSplit
}

func isModelRoleSelector(model string) bool {
	return strings.HasPrefix(strings.TrimSpace(model), "@")
}

func shouldAllowHome(cwd string) bool {
	homeDir := homeDir()
	cleanCWD := filepath.Clean(cwd)
	return cleanCWD == filepath.Clean(homeDir)
}

func parentSessionIDForFresh(sessionID string, continueFlag bool, parentSessionID string) string {
	if sessionID != "" || continueFlag {
		return ""
	}
	return parentSessionID
}

func buildOmpArgs(prompt, modelName, thinking, cwd string, yolo bool, maxTime, sessionID string, continueFlag, allowHome, steerable bool, parentSessionID string) []string {
	var cmdArgs []string
	if steerable {
		cmdArgs = append(cmdArgs, "--mode=rpc")
	} else {
		cmdArgs = append(cmdArgs, "-p")
	}
	if modelName != "" {
		cmdArgs = append(cmdArgs, fmt.Sprintf("--model=%s", modelName))
	}
	if thinking != "" {
		cmdArgs = append(cmdArgs, fmt.Sprintf("--thinking=%s", thinking))
	}
	if yolo {
		cmdArgs = append(cmdArgs, "--auto-approve")
	}
	if cwd != "" {
		cmdArgs = append(cmdArgs, fmt.Sprintf("--cwd=%s", cwd))
	}
	if maxTime != "" {
		cmdArgs = append(cmdArgs, fmt.Sprintf("--max-time=%s", maxTime))
	}
	if allowHome {
		cmdArgs = append(cmdArgs, "--allow-home")
	}
	if sessionID != "" {
		cmdArgs = append(cmdArgs, "-r", sessionID)
	} else if continueFlag {
		cmdArgs = append(cmdArgs, "-c")
	}

	if sessionID == "" && !continueFlag && parentSessionID != "" {
		cmdArgs = append(cmdArgs, "--parent-session-id", parentSessionID)
	}
	if !steerable {
		cmdArgs = append(cmdArgs, prompt)
	}
	return cmdArgs
}

func sessionDirCandidates(homeDir, cwd string) []string {
	var out []string
	seen := make(map[string]bool, 2)
	add := func(name string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		out = append(out, name)
	}
	if cwd != "" && homeDir != "" {
		if rel, err := filepath.Rel(homeDir, cwd); err == nil && rel != ".." && !strings.HasPrefix(rel, "../") {
			if rel == "." {
				add("-")
			} else {
				add("-" + strings.ReplaceAll(rel, "/", "-"))
			}
		}
	}
	add(strings.ReplaceAll(cwd, "/", "-"))
	return out
}

func findSessionFile(homeDir, effectiveCWD, sessionID string, startTime time.Time) string {
	if homeDir == "" {
		homeDir, _ = os.UserHomeDir()
	}
	sessionsBaseDir := filepath.Join(homeDir, ".omp", "agent", "sessions")

	if sessionID != "" {

		for _, cwdSlug := range sessionDirCandidates(homeDir, effectiveCWD) {
			sessionDir := filepath.Join(sessionsBaseDir, cwdSlug)
			if entries, err := os.ReadDir(sessionDir); err == nil {
				for _, e := range entries {
					if !e.IsDir() && strings.HasSuffix(e.Name(), ".jsonl") && strings.Contains(e.Name(), sessionID) {
						return filepath.Join(sessionDir, e.Name())
					}
				}
			}
		}

		if dirEntries, err := os.ReadDir(sessionsBaseDir); err == nil {
			for _, de := range dirEntries {
				if de.IsDir() {
					subDir := filepath.Join(sessionsBaseDir, de.Name())
					if files, err := os.ReadDir(subDir); err == nil {
						for _, f := range files {
							if !f.IsDir() && strings.HasSuffix(f.Name(), ".jsonl") && strings.Contains(f.Name(), sessionID) {
								return filepath.Join(subDir, f.Name())
							}
						}
					}
				}
			}
		}
	}

	if effectiveCWD == "" {
		effectiveCWD = homeDir
	}

	type fileInfo struct {
		path    string
		modTime time.Time
	}
	var files []fileInfo
	for _, cwdSlug := range sessionDirCandidates(homeDir, effectiveCWD) {
		sessionDir := filepath.Join(sessionsBaseDir, cwdSlug)
		entries, err := os.ReadDir(sessionDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".jsonl") {
				fp := filepath.Join(sessionDir, e.Name())
				if stat, err := os.Stat(fp); err == nil {
					files = append(files, fileInfo{path: fp, modTime: stat.ModTime()})
				}
			}
		}
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].modTime.After(files[j].modTime)
	})
	for _, f := range files {
		if f.modTime.After(startTime.Add(-5 * time.Second)) {
			return f.path
		}
	}

	return ""
}

func (j *Job) refreshSessionMetadata() {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.recovered {
		return
	}
	if j.Machine != "" && j.Machine != localMachineName() {
		return
	}

	if j.Steerable && (j.Status == "running" || j.rpc != nil) {
		return
	}
	if j.SessionMetadataFinalized {
		return
	}
	if j.Status != "running" {

		j.SessionMetadataFinalized = true
	}

	homeDir := homeDir()

	effectiveCWD := j.CWD
	if effectiveCWD == "" {
		effectiveCWD = homeDir
	}

	sessionFilePath := findSessionFile(homeDir, effectiveCWD, j.SessionID, j.StartTime)

	changedFilesMap := make(map[string]bool)
	for _, f := range j.FilesChanged {
		changedFilesMap[f] = true
	}

	if sessionFilePath != "" {
		file, err := os.Open(sessionFilePath)
		if err == nil {
			defer file.Close()
			scanner := bufio.NewScanner(file)

			buf := make([]byte, 1024*1024)
			scanner.Buffer(buf, 1024*1024)

			var printSwitches []modelSwitch

			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if line == "" {
					continue
				}

				if strings.Contains(line, `"type":"session"`) {
					var sessHeader struct {
						ID string `json:"id"`
					}
					if err := json.Unmarshal([]byte(line), &sessHeader); err == nil && sessHeader.ID != "" {
						if j.SessionID == "" {
							j.SessionID = sessHeader.ID
						}
					}
				}

				if !j.Steerable {
					var entry struct {
						Type                    string `json:"type"`
						Model                   string `json:"model"`
						ThinkingLevel           string `json:"thinkingLevel"`
						ResolvedModelIsFallback bool   `json:"resolvedModelIsFallback"`
					}
					if json.Unmarshal([]byte(line), &entry) == nil {

						switch entry.Type {
						case "model_change":
							if entry.Model != "" {
								j.Model, j.ModelResolved = entry.Model, entry.Model
								j.Fallback = entry.ResolvedModelIsFallback
								j.MetadataVersion = metadataVersion
							}
						case "thinking_level_change":
							j.Thinking = observedThinkingLevel(entry.ThinkingLevel, "")
							j.MetadataVersion = metadataVersion
						case "model_switch":
							var hop modelSwitch
							if json.Unmarshal([]byte(line), &hop) == nil && hop.To != "" {
								printSwitches = append(printSwitches, hop)
							}
						}
					}
				}

				if strings.Contains(line, `"tool_execution_start"`) || strings.Contains(line, `"toolName"`) {
					var toolEv struct {
						Data struct {
							ToolName string         `json:"toolName"`
							Args     map[string]any `json:"args"`
						} `json:"data"`
					}
					if err := json.Unmarshal([]byte(line), &toolEv); err == nil && toolEv.Data.ToolName != "" {
						j.LastTool = toolEv.Data.ToolName

						switch toolEv.Data.ToolName {
						case "write":
							if p, ok := toolEv.Data.Args["path"].(string); ok && p != "" {
								changedFilesMap[p] = true
							}
						case "edit":
							if input, ok := toolEv.Data.Args["input"].(string); ok && input != "" {
								matches := editPathRegex.FindAllStringSubmatch(input, -1)
								for _, m := range matches {
									if len(m) > 1 {
										changedFilesMap[m[1]] = true
									}
								}
							}
						case "ast_edit":
							if paths, ok := toolEv.Data.Args["paths"].([]any); ok {
								for _, p := range paths {
									if ps, ok := p.(string); ok && ps != "" {
										changedFilesMap[ps] = true
									}
								}
							}
						}
					}
				}

				if strings.Contains(line, `"toolName":"todo"`) && strings.Contains(line, `"toolResult"`) {
					if matches := todoOverallRe.FindStringSubmatch(line); len(matches) == 4 {
						done, _ := strconv.Atoi(matches[1])
						open, _ := strconv.Atoi(matches[3])
						j.TodosDone = done
						j.TodosOpen = open
					} else if matches := todoRemainRe.FindStringSubmatch(line); len(matches) == 2 {
						remain, _ := strconv.Atoi(matches[1])
						j.TodosOpen = remain
					}
				}
			}

			if !j.Steerable && len(printSwitches) > 0 && j.applyModelReportLocked(rpcModelReport{Switches: printSwitches}) {
				j.MetadataVersion = metadataVersion
			}
		}
	}

	if effectiveCWD != "" {
		gitCmd := exec.Command("git", "status", "--porcelain")
		gitCmd.Dir = effectiveCWD
		if out, err := gitCmd.Output(); err == nil {
			lines := strings.Split(string(out), "\n")
			for _, l := range lines {
				l = strings.TrimSpace(l)
				if len(l) > 3 {
					path := strings.TrimSpace(l[3:])
					if idx := strings.Index(path, " -> "); idx != -1 {
						path = path[idx+4:]
					}
					if path != "" {
						changedFilesMap[path] = true
					}
				}
			}
		}
	}

	var filesList []string
	for f := range changedFilesMap {
		filesList = append(filesList, f)
	}
	sort.Strings(filesList)
	j.FilesChanged = filesList
}

func (j *Job) getReport(maxChars int) (string, bool) {
	j.mu.RLock()
	effectiveCWD := j.CWD
	logPath := j.LogPath
	status := j.Status
	finalText := j.Report
	remote := j.Machine != "" && j.Machine != localMachineName()
	rpc := j.Steerable
	j.mu.RUnlock()

	if rpc {
		return headCut(sanitizeSecretString(finalText), maxChars)
	}

	if effectiveCWD == "" {
		effectiveCWD = homeDir()
	}

	var reportCandidates []string
	if !remote && !rpc {
		reportCandidates = []string{
			filepath.Join(effectiveCWD, "REPORT.md"),
			filepath.Join(effectiveCWD, "report.md"),
		}
	}
	var reportContent string
	source := "none"
	for _, cand := range reportCandidates {
		if data, err := os.ReadFile(cand); err == nil && len(data) > 0 {
			reportContent = string(data)
			source = "report"
			break
		}
	}

	if reportContent == "" && finalText != "" {
		reportContent = finalText
		source = "final"
	}

	if reportContent == "" && logPath != "" {
		if file, err := os.Open(logPath); err == nil {
			defer file.Close()
			stat, err := file.Stat()
			if err == nil {
				size := stat.Size()
				readBytes := int64(maxChars * 2)
				if readBytes > size {
					readBytes = size
				}
				offset := size - readBytes
				buf := make([]byte, readBytes)
				if _, err := file.ReadAt(buf, offset); err == nil {
					reportContent = string(buf)
					source = "log"
				}
			}
		}
	}

	if reportContent == "" {
		reportContent = fmt.Sprintf("No report generated. Status: %s.", status)
		source = "none"
	}

	reportContent = sanitizeSecretString(reportContent)

	var truncated bool
	if source == "log" {
		reportContent, truncated = tailCut(reportContent, maxChars)
	} else {
		reportContent, truncated = headCut(reportContent, maxChars)
	}
	return reportContent, truncated
}

func getStringArg(args map[string]any, key, def string) string {
	if args == nil {
		return def
	}
	if v, ok := args[key]; ok {
		if s, ok := v.(string); ok && s != "" {
			return strings.TrimSpace(s)
		}
	}
	return def
}

func getJobID(args map[string]any) string {
	if id := getStringArg(args, "job_id", ""); id != "" {
		return id
	}
	return getStringArg(args, "id", "")
}

func getBoolArg(args map[string]any, key string, def bool) bool {
	if args == nil {
		return def
	}
	if v, ok := args[key]; ok {
		switch val := v.(type) {
		case bool:
			return val
		case string:
			lower := strings.ToLower(strings.TrimSpace(val))
			return lower == "true" || lower == "1" || lower == "yes"
		}
	}
	return def
}

func getIntArg(args map[string]any, key string, def int) int {
	if args == nil {
		return def
	}
	if v, ok := args[key]; ok {
		switch val := v.(type) {
		case float64:
			return int(val)
		case int:
			return val
		case int64:
			return int(val)
		case json.Number:
			if n, err := val.Int64(); err == nil {
				return int(n)
			}
		case string:
			if n, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
				return n
			}
		}
	}
	return def
}

func promptSourceArg(args map[string]any) (*PromptSource, error) {
	raw := args["source"]
	if raw == nil {
		return nil, nil
	}
	value, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("source must be an object with kind agent and a non-empty from")
	}
	kind, _ := value["kind"].(string)
	from, _ := value["from"].(string)
	if kind != "agent" || strings.TrimSpace(from) == "" {
		return nil, fmt.Errorf("source requires kind agent and a non-empty from")
	}
	source := &PromptSource{Kind: kind, From: from}
	fields := [...]struct {
		key  string
		dest *string
	}{{"name", &source.Name}, {"title", &source.Title}, {"route", &source.Route}}
	for _, field := range fields {
		if raw, exists := value[field.key]; exists {
			text, valid := raw.(string)
			if !valid {
				return nil, fmt.Errorf("source.%s must be a string", field.key)
			}
			*field.dest = text
		}
	}
	switch source.Route {
	case "", "omp", "supervisor", "grok-webhook":
	default:
		return nil, fmt.Errorf("source.route must be omp, supervisor, or grok-webhook")
	}
	return source, nil
}

func registerTools(s *server.MCPServer) {
	sourceOption := mcp.WithObject("source",
		mcp.Description("Peer provenance: {kind: agent, from: sender identity}, with optional display-only name/title/route. Omit for human input; labels never change authorship. Agent-origin jobs use RPC to preserve provenance."),
		mcp.Properties(map[string]any{
			"kind":  map[string]any{"type": "string", "enum": []string{"agent"}},
			"from":  map[string]any{"type": "string", "minLength": 1},
			"name":  map[string]any{"type": "string"},
			"title": map[string]any{"type": "string"},
			"route": map[string]any{"type": "string", "enum": []string{"omp", "supervisor", "grok-webhook"}},
		}),
		func(schema map[string]any) { schema["required"] = []string{"kind", "from"} },
	)

	runTool := mcp.NewTool("run",
		mcp.WithDescription("Launch, resume or continue a child OMP process on a machine from machines (launch targets, not live session IDs). Remote jobs use RPC and remain steerable."),
		mcp.WithString("machine", mcp.Description("Exact id from machines; omitted uses the machine machines marks default (the Supervisor host when none is). Not an agent_id from agents. Unknown machines fail, never fall back locally.")),
		mcp.WithString("prompt", mcp.Required(), mcp.Description("Prompt to execute")),
		sourceOption,
		mcp.WithString("model", mcp.Description("Same model menu as the task tool, checked against the destination machine; omitted = the destination's @default.")),
		mcp.WithString("thinking", mcp.Description("One of the chosen model's levels; omitted = the role or model default (no injected level).")),
		mcp.WithString("cwd", mcp.Description("Working directory for the child process")),
		mcp.WithBoolean("yolo", mcp.Description("Auto-approve all tool calls (default: false)")),
		mcp.WithString("max_time", mcp.Description("Maximum duration from the job's start, 1s to 24h (e.g. 600s, 10m, 1h); extend changes it while the job runs")),
		mcp.WithString("title", mcp.Description("Descriptive title for tracking")),
		mcp.WithString("session_id", mcp.Description("Resume an existing OMP session by session ID")),
		mcp.WithString("resume", mcp.Description("Alias for session_id")),
		mcp.WithBoolean("continue", mcp.Description("Continue the previous session in the working directory (-c)")),
		mcp.WithBoolean("steerable", mcp.Description("Run the child in OMP RPC mode (omp --mode rpc) so it can be steered mid-run with steer/message (default: false = print mode)")),
		mcp.WithString("parent_session_id", mcp.Description("Owning parent session identity (linkage only)")),
		mcp.WithString("root_session_id", mcp.Description("Root session identity (linkage only)")),
		mcp.WithString("parent_agent_id", mcp.Description("Parent agent identity (linkage only)")),
		mcp.WithBoolean("notify_on_completion", mcp.Description("Opt in to one completion webhook to the exact configured grok-* parent_agent_id (default: false)")),
		mcp.WithString("parent_tool_call_id", mcp.Description("Spawning parent tool call identity (linkage only)")),
	)
	s.AddTool(runTool, handleRun)

	s.AddTool(mcp.NewTool("machines",
		mcp.WithDescription("List launch targets: the Supervisor host and every administrator-configured remote alias, even without live sessions. Returns machine ids for run.machine, optional declared host metadata, default_cwd and supported launch/session capabilities. Configuration is not proof of reachability; this does not probe hosts. Use agents for registered sessions and running steerable jobs."),
	), handleMachines)

	statusTool := mcp.NewTool("status",
		mcp.WithDescription("Check status of a child OMP process by job_id."),
		mcp.WithString("job_id", mcp.Description("Job ID returned by run (preferred)")),
		mcp.WithString("id", mcp.Description("Deprecated alias for job_id")),
	)
	s.AddTool(statusTool, handleStatus)

	s.AddTool(mcp.NewTool("events",
		mcp.WithDescription("Read safe child RPC events from the durable job log. Cursors are newline byte offsets; reset requires replay from zero. Missing/non-RPC logs return errors."),
		mcp.WithString("job_id", mcp.Required(), mcp.Description("Job ID returned by run")),
		mcp.WithNumber("after", mcp.Description("Exclusive byte cursor returned as next_cursor (default 0)")),
		mcp.WithNumber("limit", mcp.Description("Maximum events per page (default 100, bounded 1..500)")),
	), handleEvents)

	s.AddTool(mcp.NewTool("jobs",
		mcp.WithDescription("List all retained Supervisor jobs, including terminal and non-steerable jobs. Read-only metadata snapshots, newest first with a stable job-ID tie-breaker; no log recovery. Each row carries session_id (the OMP resume handle) and continued_from/continued_by lineage links."),
		mcp.WithNumber("limit", mcp.Description("Maximum jobs per page (default 50, bounded 1..200)")),
		mcp.WithNumber("offset", mcp.Description("Nonnegative page offset (default 0); continue with next_offset")),
	), handleJobs)

	resultTool := mcp.NewTool("result",
		mcp.WithDescription("Get final result/report of a completed or running child OMP process by job_id."),
		mcp.WithString("job_id", mcp.Description("Job ID returned by run (preferred)")),
		mcp.WithString("id", mcp.Description("Deprecated alias for job_id")),
		mcp.WithNumber("max_chars", mcp.Description("Max characters for report (default: 4000, max: 8000)")),
	)
	s.AddTool(resultTool, handleResult)

	cancelTool := mcp.NewTool("cancel",
		mcp.WithDescription("Cancel a running child OMP process by job_id, or drop a queued follow-up by schedule_id."),
		mcp.WithString("job_id", mcp.Description("Job ID returned by run (preferred)")),
		mcp.WithString("id", mcp.Description("Deprecated alias for job_id")),
		mcp.WithString("schedule_id", mcp.Description("Schedule ID returned by schedule: cancels the queued follow-up instead of a job")),
	)
	s.AddTool(cancelTool, handleCancel)

	s.AddTool(mcp.NewTool("extend",
		mcp.WithDescription("Change a running job's time limit. Pass exactly one of max_time (new total limit measured from the job's start) or add (time added to the current deadline). The total must stay within 1s to 24h and the new deadline must be in the future. Returns job_id, max_time and deadline (RFC3339 UTC)."),
		mcp.WithString("job_id", mcp.Required(), mcp.Description("Job ID returned by run")),
		mcp.WithString("max_time", mcp.Description("New total limit from the job's start (e.g. 6h, 90m)")),
		mcp.WithString("add", mcp.Description("Duration added to the current deadline (e.g. 2h, 30m)")),
	), handleExtend)

	modelsTool := mcp.NewTool("models",
		mcp.WithDescription("Model selectors valid on one launch target: its role selectors (roles: @role resolves to that selector there), its retry fallback chains (fallback_chains: the models a failing role walks, each entry's :level replacing the requested effort), and its model catalog. Empty query returns providers; query/provider returns selectable IDs with supported thinking levels."),
		mcp.WithString("machine", mcp.Description("Exact id from machines; omitted uses the same default launch target as run")),
		mcp.WithString("query", mcp.Description("Substring match against provider, id, selector, and display name")),
		mcp.WithString("provider", mcp.Description("Restrict to one provider, e.g. cursor or google-antigravity")),
		mcp.WithNumber("limit", mcp.Description("Max models to return (default 25, max 50)")),
	)
	s.AddTool(modelsTool, handleModels)

	sessionsTool := mcp.NewTool("sessions",
		mcp.WithDescription("Search saved OMP conversations on the Supervisor host by title, cwd, model, or snippet. Returns resume-ready session_id values; not remote host inventory (machines) or live sessions (agents)."),
		mcp.WithString("query", mcp.Description("Substring match against title, cwd, model, session id, and first user prompt")),
		mcp.WithString("cwd", mcp.Description("Restrict to sessions whose working directory contains this string")),
		mcp.WithNumber("limit", mcp.Description("Max sessions to return (default 20, max 50)")),
	)
	s.AddTool(sessionsTool, handleSessions)

	sessionTool := mcp.NewTool("session",
		mcp.WithDescription("Read one saved OMP conversation by session_id from the machine that holds it: the machine argument, else the machine of the live agent or job with that session, else the Supervisor host. Never returns a different session; errors when the conversation cannot be read. Accepts bare IDs or AgentsView omp: prefixes. Does not accept job IDs."),
		mcp.WithString("session_id", mcp.Description("OMP session ID, optionally prefixed with omp:")),
		mcp.WithString("machine", mcp.Description("Exact id from machines; omitted uses the machine its live agent or job reports, else the Supervisor host")),
		mcp.WithNumber("max_messages", mcp.Description("Max user/assistant messages (default 20, max 40)")),
		mcp.WithNumber("max_chars", mcp.Description("Max total characters across messages (default 4000, max 8000)")),
	)
	s.AddTool(sessionTool, handleSession)

	steerDescription := "Send input into an OMP job or registered session. Explicit mode selects steer, non-aborting aside, or follow_up after the current turn. A finished (done/error/canceled) job is continued: the message launches a fresh job that resumes its session, inheriting machine, cwd, model, and effort  -  jobs are reusable, no manual session resume needed. One-shot webhook peers are not steerable; use prompt with their exact ID."
	steerOptions := []mcp.ToolOption{
		mcp.WithDescription(steerDescription),
		mcp.WithString("job_id", mcp.Description("Job ID returned by run (preferred)")),
		mcp.WithString("id", mcp.Description("Deprecated alias for job_id")),
		mcp.WithString("agent_id", mcp.Description("Live agent ID from agents: steers a registered session (local TTY, Mac, another host) instead of a job")),
		mcp.WithString("text", mcp.Required(), mcp.Description("Message to deliver (message and prompt are accepted aliases)")),
		mcp.WithString("mode", mcp.Description("steer = interrupt now (default); aside = peer input without aborting; follow_up = after the current turn")),
		mcp.WithString("sender_session_id", mcp.Description("Runtime-owned; do not set. The OMP runtime fills in the caller's own session id, and a target resolving to it is refused")),
		sourceOption,
	}
	s.AddTool(mcp.NewTool("steer", steerOptions...), handleSteer)
	s.AddTool(mcp.NewTool("message", steerOptions...), handleSteer)

	scheduleTool := mcp.NewTool("schedule",
		mcp.WithDescription("Queue a prompt to run as a new child job once the target job reaches a terminal state (done/error/canceled). Survives supervisor restarts. If the target already finished, steer/message to it instead  -  that continues its session immediately."),
		mcp.WithString("job_id", mcp.Description("Job ID to wait for (id is accepted as a deprecated alias)")),
		mcp.WithString("id", mcp.Description("Deprecated alias for job_id")),
		mcp.WithString("prompt", mcp.Required(), mcp.Description("Prompt for the follow-up job")),
		sourceOption,
		mcp.WithString("model", mcp.Description("Model for the follow-up job, checked against the destination machine like run (default: the target job's requested selector, so a role stays a role)")),
		mcp.WithString("thinking", mcp.Description("Thinking level for the follow-up job (default: the target job's requested level when the model is inherited, else the role or model default)")),
		mcp.WithString("cwd", mcp.Description("Working directory (default: the target job's cwd)")),
		mcp.WithBoolean("yolo", mcp.Description("Auto-approve all tool calls in the follow-up job (default: false)")),
		mcp.WithString("max_time", mcp.Description("Maximum duration for the follow-up job (e.g. 600s, 10m)")),
		mcp.WithString("title", mcp.Description("Descriptive title for the follow-up job")),
		mcp.WithBoolean("steerable", mcp.Description("Start the follow-up job in RPC mode so it can be steered (default: false)")),
		mcp.WithBoolean("resume", mcp.Description("Resume the target job's OMP session so the follow-up keeps its context (default: true)")),
		mcp.WithString("parent_agent_id", mcp.Description("Originating agent for this follow-up (linkage only; not inherited from the target job)")),
		mcp.WithBoolean("notify_on_completion", mcp.Description("Opt in to one completion webhook to an exact configured grok-* parent_agent_id (default: false; not inherited)")),
	)
	s.AddTool(scheduleTool, handleSchedule)

	agentsTool := mcp.NewTool("agents",
		mcp.WithDescription("List live OMP sessions: registered agents from any host plus running steerable children this Supervisor spawned. machine is the execution alias; optional host is client-reported identity. state is busy (mid-turn), idle (waiting for input) or unknown; last_turn_ended_at is when its last turn finished and busy_since when the current one started. Use machines for launch targets. Explicit include_grok adds configured-only webhook peers; these are not running sessions."),
		mcp.WithString("query", mcp.Description("Substring match against agent id, name, machine, host hostname/platform/architecture, session id, cwd, model, and title")),
		mcp.WithString("machine", mcp.Description("Restrict live sessions by machine alias substring; use machines to discover exact launch target ids")),
		mcp.WithBoolean("include_stale", mcp.Description("Include registrations that stopped heartbeating (default: false)")),
		mcp.WithBoolean("include_grok", mcp.Description("Explicitly include configured grok-* webhook peers (default: false); configuration is not observed execution or liveness")),
		mcp.WithNumber("limit", mcp.Description("Max agents to return (default 25, max 100)")),
	)
	s.AddTool(agentsTool, handleAgents)

	agentTool := mcp.NewTool("agent",
		mcp.WithDescription("Show one OMP agent or an explicitly addressed configured grok-* webhook peer. Default is a summary (counts + short previews). Set include_queue/include_history for the full OMP lists; for a grok-* peer, history lists its retained asks (request_id, status, answer) and request_id fetches one ask with its full answer."),
		mcp.WithString("agent_id", mcp.Required(), mcp.Description("OMP agent ID (session_id, name, or job ID also resolve), or an exact configured grok-* ID")),
		mcp.WithBoolean("revoke", mcp.Description("Drop the registration and invalidate its agent token (default: false)")),
		mcp.WithBoolean("include_queue", mcp.Description("Include the full queued prompt list (default: false; each item capped at max_chars)")),
		mcp.WithBoolean("include_history", mcp.Description("Include acknowledged deliveries, or a grok-* peer's retained asks (default: false; each item capped at max_chars)")),
		mcp.WithString("request_id", mcp.Description("grok-* peers only: return this ask (from prompt's request_id or the history list) with its status and full answer")),
		mcp.WithNumber("max_chars", mcp.Description("Max characters per included queue/history item (default: 4000, max: 8000)")),
	)
	s.AddTool(agentTool, handleAgent)

	promptTool := mcp.NewTool("prompt",
		mcp.WithDescription("Send a prompt into an OMP session. Registered agents poll their inbox; Supervisor children use RPC. Only an exact configured grok-* ID explicitly selects a one-shot webhook and waits for its callback; display names and generic aliases never select Grok. A grok ask is stored before sending and returns a request_id; if no answer arrives within wait_seconds it stays open, and a late answer is retrievable with agent{agent_id, request_id} without resending."),
		mcp.WithString("agent_id", mcp.Required(), mcp.Description("OMP agent ID (session_id, name, or job ID also resolve), or an exact configured grok-* ID")),
		mcp.WithString("text", mcp.Required(), mcp.Description("Prompt to deliver (message and prompt are accepted aliases)")),
		mcp.WithString("mode", mcp.Description("OMP only: steer = interrupt (default), aside = non-aborting peer input, follow_up = after the current turn. Webhook prompts have no session queue semantics.")),
		mcp.WithNumber("wait_seconds", mcp.Description("How long to wait for the agent to pick the prompt up (default 10, 0 = return immediately). grok-* asks: how long to wait for the answer (default 120, max 300)")),
		mcp.WithString("wait_for", mcp.Description("delivered = wait for pickup (default); ack = wait for the agent's reply/result; none = do not wait (grok-* asks return once the webhook accepted the ask)")),
		mcp.WithString("from", mcp.Description("Optional label for who sent this prompt (shown in the agent's delivery history)")),
		mcp.WithString("sender_session_id", mcp.Description("Runtime-owned; do not set. The OMP runtime fills in the caller's own session id, and a target resolving to it is refused so a session never prompts itself")),
		sourceOption,
	)
	s.AddTool(promptTool, handlePrompt)

	s.AddTool(mcp.NewTool("ack_capture",
		mcp.WithDescription("Acknowledge a finished child capture with the exact parent linkage and observed child session id. Persists the ack, then archives the raw log off-host when SUPERVISOR_CAPTURE_ARCHIVE_TARGET is set. Private MCP only."),
		mcp.WithString("job_id", mcp.Required(), mcp.Description("Job ID returned by run")),
		mcp.WithString("parent_session_id", mcp.Required(), mcp.Description("Parent session id stored on the job")),
		mcp.WithString("root_session_id", mcp.Required(), mcp.Description("Root session id stored on the job")),
		mcp.WithString("parent_agent_id", mcp.Required(), mcp.Description("Parent agent id stored on the job")),
		mcp.WithString("parent_tool_call_id", mcp.Required(), mcp.Description("Parent tool call id stored on the job")),
		mcp.WithString("session_id", mcp.Required(), mcp.Description("Observed child session id stored on the job")),
	), handleAckCapture)

	s.AddTool(mcp.NewTool("restore_capture",
		mcp.WithDescription("Restore an archived child capture to its exact log path after compressed and original checksums match. Does not delete the remote copy. Private MCP only."),
		mcp.WithString("job_id", mcp.Required(), mcp.Description("Job ID returned by run")),
	), handleRestoreCapture)
}

type jobSpec struct {
	Machine            string
	Prompt             string
	Source             *PromptSource
	RawModel           string
	Thinking           string
	CWD                string
	MaxTime            string
	Title              string
	SessionID          string
	ParentSessionID    string
	RootSessionID      string
	ParentAgentID      string
	NotifyOnCompletion bool
	ParentToolCallID   string
	Yolo               bool
	Continue           bool
	Steerable          bool
}

func startJob(spec jobSpec) (*Job, error) {

	store, schedules := globalStore, globalSchedules
	if spec.Prompt == "" {
		return nil, fmt.Errorf("prompt is required")
	}
	if err := validateCompletionNotification(spec.NotifyOnCompletion, spec.ParentAgentID); err != nil {
		return nil, err
	}

	if strings.TrimSpace(spec.Machine) == "" {

		machine, err := defaultLaunchMachine()
		if err != nil {
			return nil, err
		}
		spec.Machine = machine
	}
	remote, err := resolveRemoteMachine(spec.Machine)
	if err != nil {
		return nil, err
	}
	if remote != nil || spec.Source != nil {
		spec.Steerable = true
	}
	maxTime := spec.MaxTime
	if maxTime == "" && remote != nil {
		maxTime = remoteMaxTime
	}
	var limit time.Duration
	ceiling := ""
	if maxTime != "" {
		var err error
		if limit, err = parseMaxTime(maxTime); err != nil {
			return nil, err
		}
		ceiling = maxTimeCeiling
	}
	if remote == nil {
		spec.Machine = localMachineName()
	}
	rawModel := strings.TrimSpace(spec.RawModel)
	modelName, thinking, _ := parseModelAndThinking(rawModel, spec.Thinking)
	if rawModel == "" || rawModel == defaultModelSelector {

		rawModel = defaultModelSelector
		modelName = ""
		thinking = spec.Thinking
	}

	sessionID := normalizeSessionID(spec.SessionID)
	if sessionID != "" {
		if runningJob := store.findRunningJobBySessionID(sessionID); runningJob != nil {
			return nil, fmt.Errorf("session %s is already running in job %s (pid %d)", sessionID, runningJob.ID, runningJob.PID)
		}
	}

	policyViolation, err := checkLaunchPolicy(spec.Machine, remote, rawModel, spec.Thinking)
	if err != nil {
		return nil, err
	}

	var cwd string
	if remote != nil {
		cwd = spec.CWD
		if cwd == "" {
			cwd = remote.Home
		}
	} else {
		cwd, err = resolveJobCwd(spec.CWD)
		if err != nil {
			return nil, err
		}
	}

	title := spec.Title
	if title == "" {
		title = extractTitle(spec.Prompt)
	}

	jobID := generateJobID()
	logPath := filepath.Join(store.logsDir, fmt.Sprintf("%s.log", jobID))

	execCtx, cancelFn := context.WithCancel(context.Background())
	var cmd *exec.Cmd
	var remoteInit []byte
	if remote != nil {
		cmd, err = remoteCommand(execCtx, remote)
		if err != nil {
			cancelFn()
			return nil, err
		}
		remoteInit, err = json.Marshal(map[string]any{
			"cwd": cwd, "model": modelName, "thinking": thinking,
			"max_time": maxTimeCeiling, "session_id": sessionID, "continue": spec.Continue,
			"parent_session_id": parentSessionIDForFresh(sessionID, spec.Continue, spec.ParentSessionID),
			"yolo":              spec.Yolo,
		})
		if err != nil {
			cancelFn()
			return nil, err
		}
		remoteInit = append(remoteInit, '\n')
	} else {
		ompBin, lookupErr := locateOmpBinary()
		if lookupErr != nil {
			cancelFn()
			return nil, lookupErr
		}
		parentSessionID := spec.ParentSessionID
		if parentSessionID != "" && !ompSupportsParentSessionID(ompBin) {
			parentSessionID = ""
		}
		cmdArgs := buildOmpArgs(spec.Prompt, modelName, thinking, cwd, spec.Yolo, ceiling, sessionID, spec.Continue, shouldAllowHome(cwd), spec.Steerable, parentSessionID)
		cmd = exec.CommandContext(execCtx, ompBin, cmdArgs...)
		cmd.Dir = cwd
	}

	homeDir := homeDir()
	env := os.Environ()
	hasHome := false
	hasPath := false
	for _, e := range env {
		if strings.HasPrefix(e, "HOME=") {
			hasHome = true
		}
		if strings.HasPrefix(e, "PATH=") {
			hasPath = true
		}
	}
	if !hasHome {
		env = append(env, "HOME="+homeDir)
	}
	if p := childPathEnv(hasPath); p != "" {
		env = append(env, p)
	}

	env = append(env, ompModelPolicyEnv)
	cmd.Env = env
	configureChildProcess(cmd)

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		cancelFn()
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	job := &Job{
		Machine:            spec.Machine,
		ID:                 jobID,
		SessionID:          sessionID,
		ParentSessionID:    spec.ParentSessionID,
		RootSessionID:      spec.RootSessionID,
		ParentAgentID:      spec.ParentAgentID,
		NotifyOnCompletion: spec.NotifyOnCompletion,
		ParentToolCallID:   spec.ParentToolCallID,
		MetadataVersion:    metadataVersion,
		Model:              rawModel,
		ModelRequested:     rawModel,
		Fallback:           false,
		ThinkingRequested:  thinking,
		PolicyViolations:   nonEmpty(policyViolation),
		Prompt:             spec.Prompt,
		Source:             spec.Source,
		Title:              title,
		CWD:                cwd,
		Yolo:               spec.Yolo,
		MaxTime:            maxTime,
		LogPath:            logPath,
		Status:             "running",
		Steerable:          spec.Steerable,
		StartTime:          time.Now(),
		cmd:                cmd,
		cancelFn:           cancelFn,
		monitorDone:        make(chan struct{}),
	}
	if spec.Steerable {
		job.captureDone = make(chan struct{})
	}

	var stdin io.WriteCloser
	var stdout io.ReadCloser
	var stderrFile *os.File
	if spec.Steerable {
		if stdin, err = cmd.StdinPipe(); err != nil {
			logFile.Close()
			cancelFn()
			return nil, fmt.Errorf("failed to open rpc stdin: %w", err)
		}
		if stdout, err = cmd.StdoutPipe(); err != nil {
			logFile.Close()
			cancelFn()
			return nil, fmt.Errorf("failed to open rpc stdout: %w", err)
		}

		stderrFile, err = os.OpenFile(logPath+".stderr", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			stdin.Close()
			stdout.Close()
			logFile.Close()
			cancelFn()
			return nil, fmt.Errorf("failed to open stderr log: %w", err)
		}
		cmd.Stderr = stderrFile
	} else {
		cmd.Stdout = logFile
		cmd.Stderr = logFile
	}

	if err := cmd.Start(); err != nil {
		logFile.Close()
		if stderrFile != nil {
			stderrFile.Close()
		}
		cancelFn()
		return nil, fmt.Errorf("failed to start omp: %w", err)
	}
	job.PID = cmd.Process.Pid
	if remote != nil {
		if _, err := stdin.Write(remoteInit); err != nil {
			stdin.Close()
			cancelFn()
			cmd.Wait()
			logFile.Close()
			stderrFile.Close()
			return nil, fmt.Errorf("remote bootstrap failed: %w", err)
		}
	}

	if limit > 0 {
		job.Deadline = job.StartTime.Add(limit)
	}
	store.add(job)
	job.mu.Lock()
	store.armDeadlineLocked(job)
	job.mu.Unlock()
	var child *rpcChild
	if spec.Steerable {
		job.mu.Lock()
		child = startRPCChild(store, job, stdin, stdout, logFile, spec.Prompt)
		job.rpc = child
		job.mu.Unlock()
	}

	exited := make(chan struct{})
	if child != nil {
		go reapAfterStdinEOF(job.ID, cmd.Process.Pid, child.done, exited, rpcExitGrace())
	}

	go func() {
		defer close(job.monitorDone)
		defer logFile.Close()
		if stderrFile != nil {
			defer stderrFile.Close()
		}

		if child != nil {
			<-child.readDone
		}
		err := cmd.Wait()
		close(exited)
		job.mu.RLock()
		deadlineMessage := job.deadlineMessage
		job.mu.RUnlock()
		if deadlineMessage != "" {
			if stderrFile != nil {
				fmt.Fprintln(stderrFile, deadlineMessage)
			} else {
				fmt.Fprintln(logFile, deadlineMessage)
			}
		}
		if child != nil {
			child.close()
		}

		startupFailure := ""
		if child != nil && err != nil && !child.sawPromptResult() {
			startupFailure = startupFailureReport(err, logPath+".stderr")
		}

		job.mu.Lock()
		if job.EndTime.IsZero() {
			job.EndTime = time.Now()
		}

		if job.Status == "running" {
			if err != nil {
				job.Status = "error"
				if exitErr, ok := err.(*exec.ExitError); ok {
					job.ExitCode = exitErr.ExitCode()
				} else {
					job.ExitCode = -1
				}
				if job.Report == "" {
					job.Report = startupFailure
				}
			} else if job.CaptureError != "" {

				job.Status = "error"
				job.ExitCode = -1
			} else {
				job.Status = "done"
				job.ExitCode = 0
			}
		}
		job.rpc = nil
		job.mu.Unlock()

		job.refreshSessionMetadata()
		store.save(job)
		notifyCompletionWebhooks(job)

		if schedules != nil {
			schedules.onJobTerminal(job.ID)
		}
	}()

	return job, nil
}

const defaultRPCExitGrace = 60 * time.Second

func rpcExitGrace() time.Duration {
	raw := strings.TrimSpace(os.Getenv("SUPERVISOR_RPC_EXIT_GRACE_SECONDS"))
	if raw == "" {
		return defaultRPCExitGrace
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > 3600 {
		return defaultRPCExitGrace
	}
	return time.Duration(n) * time.Second
}

func reapAfterStdinEOF(jobID string, pid int, stdinClosed <-chan struct{}, exited <-chan struct{}, grace time.Duration) {
	select {
	case <-exited:
		return
	case <-stdinClosed:
	}
	select {
	case <-exited:
		return
	case <-time.After(grace):
	}
	log.Printf("job %s: RPC child still running %s after stdin EOF; sending SIGTERM", jobID, grace)
	terminateProcessGroup(pid)
	select {
	case <-exited:
		return
	case <-time.After(5 * time.Second):
	}
	log.Printf("job %s: RPC child ignored SIGTERM; sending SIGKILL", jobID)
	killProcessGroup(pid)
}

func handleRun(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	source, err := promptSourceArg(args)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	prompt := getStringArg(args, "prompt", "")
	if prompt == "" {
		return mcp.NewToolResultError("prompt is required"), nil
	}

	sessionID := getStringArg(args, "session_id", "")
	if sessionID == "" {
		sessionID = getStringArg(args, "resume", "")
	}

	job, err := startJob(jobSpec{
		Prompt:             prompt,
		Source:             source,
		Machine:            getStringArg(args, "machine", ""),
		RawModel:           getStringArg(args, "model", ""),
		Thinking:           getStringArg(args, "thinking", ""),
		CWD:                getStringArg(args, "cwd", ""),
		MaxTime:            getStringArg(args, "max_time", ""),
		Title:              getStringArg(args, "title", ""),
		SessionID:          sessionID,
		ParentSessionID:    getStringArg(args, "parent_session_id", ""),
		RootSessionID:      getStringArg(args, "root_session_id", ""),
		ParentAgentID:      getStringArg(args, "parent_agent_id", ""),
		NotifyOnCompletion: getBoolArg(args, "notify_on_completion", false),
		ParentToolCallID:   getStringArg(args, "parent_tool_call_id", ""),
		Yolo:               getBoolArg(args, "yolo", false),
		Continue:           getBoolArg(args, "continue", false),
		Steerable:          getBoolArg(args, "steerable", false),
	})
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	job.mu.RLock()
	respData := map[string]any{
		"job_id":    job.ID,
		"id":        job.ID,
		"pid":       job.PID,
		"log_path":  job.LogPath,
		"status":    job.Status,
		"steerable": job.Steerable,
	}
	job.addExecutionMetadataLocked(respData)
	job.mu.RUnlock()

	jsonBytes, err := json.Marshal(respData)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

func handleStatus(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	id := getJobID(args)
	if id == "" {
		return mcp.NewToolResultError("job_id is required (id is accepted as a deprecated alias)"), nil
	}

	store := globalStore
	job := store.get(id)
	if job == nil {
		return mcp.NewToolResultError(fmt.Sprintf("job %q not found", id)), nil
	}
	store.ensureRecoveredMetadata(job)
	store.reconcileRecovered(job)

	job.refreshSessionMetadata()

	job.mu.RLock()
	respData := map[string]any{
		"job_id":     job.ID,
		"id":         job.ID,
		"status":     job.Status,
		"pid":        job.PID,
		"last_tool":  job.LastTool,
		"todos_done": job.TodosDone,
		"todos_open": job.TodosOpen,
		"title":      job.Title,
		"steerable":  job.Steerable && job.rpc != nil && !job.rpc.isClosed(),
	}
	if !job.StartTime.IsZero() {
		if !job.EndTime.IsZero() {
			respData["elapsed_s"] = int(job.EndTime.Sub(job.StartTime).Seconds())
		} else if job.Status == "running" {
			respData["elapsed_s"] = int(time.Since(job.StartTime).Seconds())
		}
	}
	job.addExecutionMetadataLocked(respData)
	job.mu.RUnlock()

	if globalSchedules != nil {
		var queued []map[string]any
		for _, sch := range globalSchedules.pendingFor(job.ID) {
			queued = append(queued, map[string]any{
				"schedule_id": sch.ID,
				"title":       sch.Title,
				"model":       sch.Model,
			})
		}
		if len(queued) > 0 {
			respData["scheduled"] = queued
		}
	}

	jsonBytes, err := json.Marshal(respData)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

func handleResult(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	id := getJobID(args)
	if id == "" {
		return mcp.NewToolResultError("job_id is required (id is accepted as a deprecated alias)"), nil
	}

	store := globalStore
	job := store.get(id)
	if job == nil {
		return mcp.NewToolResultError(fmt.Sprintf("job %q not found", id)), nil
	}
	store.ensureRecoveredMetadata(job)
	store.reconcileRecovered(job)

	maxChars := clampMaxChars(getIntArg(args, "max_chars", defaultMaxChars))

	job.refreshSessionMetadata()
	report, truncated := job.getReport(maxChars)

	job.mu.RLock()
	respData := map[string]any{
		"job_id":        job.ID,
		"id":            job.ID,
		"status":        job.Status,
		"exit_code":     job.ExitCode,
		"title":         job.Title,
		"report":        report,
		"files_changed": job.FilesChanged,
	}
	job.addExecutionMetadataLocked(respData)
	job.mu.RUnlock()
	if truncated {
		respData["truncated"] = true
	}

	jsonBytes, err := json.Marshal(respData)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

func (j *Job) addExecutionMetadataLocked(out map[string]any) {
	out["machine"] = j.Machine
	out["session_id"] = j.SessionID
	if j.ContinuedFromJobID != "" {
		out["continued_from_job_id"] = j.ContinuedFromJobID
	}
	if j.ContinuedByJobID != "" {
		out["continued_by_job_id"] = j.ContinuedByJobID
	}
	out["model_requested"] = j.ModelRequested
	out["title"] = j.Title
	out["session_title"] = j.SessionTitle
	if !j.StartTime.IsZero() {
		out["start_time"] = j.StartTime
	}
	if !j.EndTime.IsZero() {
		out["end_time"] = j.EndTime
	}
	if j.MaxTime != "" {
		out["max_time"] = j.MaxTime
	}
	if !j.Deadline.IsZero() {
		out["deadline"] = j.Deadline.UTC().Format(time.RFC3339)
	}

	if j.Status == "error" && j.ExitCode == 124 {
		out["termination_reason"] = "timed_out"
	}
	if j.ModelResolved != "" {
		out["model"] = j.ModelResolved
		out["model_resolved"] = j.ModelResolved
	}
	if j.ModelResolved != "" || j.Fallback {
		out["fallback"] = j.Fallback
	}
	if j.FallbackFrom != "" {
		out["fallback_from"] = j.FallbackFrom
	}
	if j.FallbackReason != "" {
		out["fallback_reason"] = j.FallbackReason
	}
	if len(j.ModelsUsed) > 0 {
		out["models_used"] = append([]string(nil), j.ModelsUsed...)
	}
	if j.Thinking != "" {
		out["thinking"] = j.Thinking
	}
	if j.ThinkingRequested != "" {
		out["thinking_requested"] = j.ThinkingRequested
	}
	if len(j.PolicyViolations) > 0 {
		out["policy_violations"] = append([]string(nil), j.PolicyViolations...)
	}
	if len(j.ModelSwitches) > 0 {
		out["model_switches"] = append([]modelSwitch(nil), j.ModelSwitches...)
	}
	if line := j.modelLineLocked(); line != "" {
		out["model_line"] = line
	}
	if j.CaptureError != "" {
		out["capture_error"] = j.CaptureError
	}
	if j.ParentSessionID != "" {
		out["parent_session_id"] = j.ParentSessionID
	}
	if j.RootSessionID != "" {
		out["root_session_id"] = j.RootSessionID
	}
	if j.ParentAgentID != "" {
		out["parent_agent_id"] = j.ParentAgentID
	}
	if j.ParentToolCallID != "" {
		out["parent_tool_call_id"] = j.ParentToolCallID
	}
}

func handleCancel(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()

	if scheduleID := getStringArg(args, "schedule_id", ""); scheduleID != "" {
		sch, err := globalSchedules.cancel(scheduleID)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		jsonBytes, mErr := json.Marshal(map[string]any{
			"schedule_id":   sch.ID,
			"target_job_id": sch.TargetJobID,
			"status":        sch.Status,
			"canceled":      true,
		})
		if mErr != nil {
			return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", mErr)), nil
		}
		return mcp.NewToolResultText(string(jsonBytes)), nil
	}

	id := getJobID(args)
	if id == "" {
		return mcp.NewToolResultError("job_id is required (id is accepted as a deprecated alias; schedule_id cancels a queued follow-up)"), nil
	}

	job := globalStore.get(id)
	if job == nil {
		return mcp.NewToolResultError(fmt.Sprintf("job %q not found", id)), nil
	}

	job.mu.Lock()
	if job.Status != "running" || !isProcessAlive(job.PID) {
		if job.Status == "running" {
			job.Status = "error"
			if job.EndTime.IsZero() {
				job.EndTime = time.Now()
			}
		}
		status := job.Status
		pid := job.PID
		sessionID := job.SessionID
		job.mu.Unlock()

		respData := map[string]any{
			"job_id":     id,
			"id":         id,
			"status":     status,
			"canceled":   false,
			"pid":        pid,
			"session_id": sessionID,
		}
		jsonBytes, err := json.Marshal(respData)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
		}
		return mcp.NewToolResultText(string(jsonBytes)), nil
	}

	pid := job.PID
	sessionID := job.SessionID
	cancelFn := job.cancelFn
	job.Status = "canceled"
	job.ExitCode = -1
	if job.EndTime.IsZero() {
		job.EndTime = time.Now()
	}
	job.mu.Unlock()

	if cancelFn != nil {
		cancelFn()
	}

	if pid > 0 {

		terminateProcessGroup(pid)

		exited := false
		for i := 0; i < 20; i++ {
			time.Sleep(100 * time.Millisecond)
			if !isProcessAlive(pid) {
				exited = true
				break
			}
		}

		if !exited && isProcessAlive(pid) {
			killProcessGroup(pid)
		}
	}

	job.refreshSessionMetadata()
	globalStore.save(job)
	notifyCompletionWebhooks(job)

	if globalSchedules != nil {
		globalSchedules.onJobTerminal(id)
	}

	respData := map[string]any{
		"job_id":     id,
		"id":         id,
		"status":     "canceled",
		"canceled":   true,
		"pid":        pid,
		"session_id": sessionID,
	}

	jsonBytes, err := json.Marshal(respData)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

func newHTTPMux(mcpHandler http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpHandler)

	mux.HandleFunc("POST /api/agents/callback/{id}", handleGrokCallback)
	mux.Handle("/api/agents/", globalAgents.registryMux())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy","service":"supervisor"}`))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"name":"Supervisor","version":"1.0.0","tools":["run","machines","status","result","events","jobs","ack_capture","restore_capture","cancel","models","sessions","session","steer","message","schedule","agents","agent","prompt"],"registry":"/api/agents/register"}`))
	})
	return mux
}

func main() {
	homeDir := homeDir()
	baseDir := os.Getenv("SUPERVISOR_DATA_DIR")
	if baseDir == "" {
		baseDir = filepath.Join(homeDir, ".omp", "supervisor")
	}
	if err := os.MkdirAll(filepath.Join(baseDir, "jobs"), 0755); err != nil {
		fallback := filepath.Join(homeDir, ".omp", "supervisor")
		if fallback != baseDir {
			log.Printf("SUPERVISOR_DATA_DIR %s not writable (%v); falling back to %s", baseDir, err, fallback)
			baseDir = fallback
		}
	}

	var err error
	globalStore, err = newJobStore(baseDir)
	if err != nil {
		log.Fatalf("Failed to initialize job store: %v", err)
	}

	globalSchedules, err = newScheduleStore(baseDir)
	if err != nil {
		log.Fatalf("Failed to initialize schedule store: %v", err)
	}
	globalSchedules.reconcile()
	globalGrokAsks, err = newGrokAskStore(baseDir)
	if err != nil {
		log.Fatalf("Failed to initialize grok ask store: %v", err)
	}
	startCaptureRetentionWorker()

	globalAgents, err = newAgentRegistry(baseDir)
	if err != nil {
		log.Fatalf("Failed to initialize agent registry: %v", err)
	}
	globalAgents.startReaper(15 * time.Second)
	if globalAgents.auth.disabled {
		log.Printf("Agent registry: registration DISABLED (SUPERVISOR_REGISTRY_DISABLED)")
	} else if globalAgents.auth.tokenPath != "" {
		verb := "reusing"
		if globalAgents.auth.generated {
			verb = "generated"
		}
		log.Printf("Agent registry: %s enrollment token in %s (set SUPERVISOR_REGISTRY_TOKEN to manage it explicitly)", verb, globalAgents.auth.tokenPath)
	} else {
		log.Printf("Agent registry: %d enrollment token(s) from the environment", len(globalAgents.auth.enrollHashes))
	}

	mcpServer := server.NewMCPServer("Supervisor", "1.0.0")
	registerTools(mcpServer)

	host := os.Getenv("FASTMCP_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("FASTMCP_PORT")
	if port == "" {
		port = "8000"
	}

	streamableServer := server.NewStreamableHTTPServer(mcpServer, server.WithStateLess(true))

	mcpAuthMode := os.Getenv("SUPERVISOR_MCP_AUTH")
	mcpHandler, authRequired, err := gateMCPHandler(streamableServer, mcpAuthMode, host, globalAgents.auth)
	if err != nil {
		log.Fatalf("%v", err)
	}
	if authRequired {
		log.Printf("MCP auth: required (SUPERVISOR_MCP_AUTH=%s, listen host %s); /mcp needs Authorization: Bearer <registry token>", mcpAuthModeLabel(mcpAuthMode), host)
	} else {
		log.Printf("MCP auth: off (SUPERVISOR_MCP_AUTH=%s, listen host %s); /mcp is unauthenticated", mcpAuthModeLabel(mcpAuthMode), host)
	}
	mux := newHTTPMux(mcpHandler)

	addr := fmt.Sprintf("%s:%s", host, port)
	log.Printf("Starting Supervisor MCP Server on %s (mcp at /mcp)", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
