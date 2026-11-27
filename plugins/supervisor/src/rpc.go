package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

type rpcChild struct {
	terminalError   string
	mu              sync.Mutex
	writeMu         sync.Mutex
	stdin           io.WriteCloser
	closed          bool
	seq             int
	initialPromptID string
	sends           int
	pending         map[string]chan rpcFrame
	ready           chan struct{}
	readyOne        sync.Once
	terminal        chan struct{}
	job             *Job
	store           *JobStore
	logFile         *os.File

	replies      chan []byte
	stateRefresh chan struct{}
	done         chan struct{}
	readDone     chan struct{}

	lastActivity time.Time
	lastSend     time.Time

	promptResultSeen bool
}

type rpcContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type rpcMessage struct {
	StopReason   string          `json:"stopReason"`
	ErrorMessage string          `json:"errorMessage"`
	Role         string          `json:"role"`
	Content      json.RawMessage `json:"content"`
}

type rpcMessages []rpcMessage

func (m *rpcMessages) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	out := make(rpcMessages, 0, len(raw))
	for _, item := range raw {
		var msg rpcMessage
		if err := json.Unmarshal(item, &msg); err != nil {

			continue
		}
		out = append(out, msg)
	}
	*m = out
	return nil
}

type rpcError string

func (e *rpcError) UnmarshalJSON(data []byte) error {
	*e = ""
	var text string
	if json.Unmarshal(data, &text) == nil {
		*e = rpcError(text)
		return nil
	}
	var obj struct {
		Message    string `json:"message"`
		Provider   string `json:"provider"`
		Model      string `json:"model"`
		HTTPStatus int    `json:"httpStatus"`
	}
	if json.Unmarshal(data, &obj) == nil && obj.Message != "" {
		var detail []string
		if source := strings.TrimSpace(obj.Provider + " " + obj.Model); source != "" {
			detail = append(detail, source)
		}
		if obj.HTTPStatus != 0 {
			detail = append(detail, fmt.Sprintf("HTTP %d", obj.HTTPStatus))
		}
		text = obj.Message
		if len(detail) > 0 {
			text += " (" + strings.Join(detail, ", ") + ")"
		}
		*e = rpcError(text)
		return nil
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, data); err == nil && compact.String() != "null" {
		*e = rpcError(compact.String())
	}
	return nil
}

type rpcFrame struct {
	Type     string   `json:"type"`
	ID       string   `json:"id"`
	Command  string   `json:"command"`
	Success  *bool    `json:"success"`
	Error    rpcError `json:"error"`
	Status   string   `json:"status"`
	Method   string   `json:"method"`
	ToolName string   `json:"toolName"`

	IsTerminal *bool `json:"isTerminal"`

	Messages rpcMessages     `json:"messages"`
	Message  json.RawMessage `json:"message"`

	MessageCount  int             `json:"messageCount"`
	Data          json.RawMessage `json:"data"`
	Resolved      string          `json:"resolved"`
	ThinkingLevel string          `json:"thinkingLevel"`

	From   json.RawMessage `json:"from"`
	Reason json.RawMessage `json:"reason"`
}

type rpcEnvelope struct {
	Type       string          `json:"type"`
	ID         string          `json:"id"`
	Command    string          `json:"command"`
	Success    *bool           `json:"success"`
	Error      rpcError        `json:"error"`
	Status     string          `json:"status"`
	Method     string          `json:"method"`
	ToolName   string          `json:"toolName"`
	IsTerminal *bool           `json:"isTerminal"`
	Data       json.RawMessage `json:"data"`
}

func decodeFrame(line []byte) (rpcFrame, error) {
	var frame rpcFrame
	if err := json.Unmarshal(line, &frame); err == nil {
		return frame, nil
	}
	var env rpcEnvelope
	if err := json.Unmarshal(line, &env); err != nil {
		return rpcFrame{}, err
	}
	return rpcFrame{
		Type:       env.Type,
		ID:         env.ID,
		Command:    env.Command,
		Success:    env.Success,
		Error:      env.Error,
		Status:     env.Status,
		Method:     env.Method,
		ToolName:   env.ToolName,
		IsTerminal: env.IsTerminal,
		Data:       env.Data,
	}, nil
}

type rpcState struct {
	Model *struct {
		Provider string `json:"provider"`
		ID       string `json:"id"`
	} `json:"model"`

	SessionID          string  `json:"sessionId"`
	SessionName        *string `json:"sessionName"`
	ThinkingLevel      string  `json:"thinkingLevel"`
	IsStreaming        bool    `json:"isStreaming"`
	IsCompacting       bool    `json:"isCompacting"`
	QueuedMessageCount int     `json:"queuedMessageCount"`

	RetryFallback json.RawMessage `json:"retryFallback"`

	ModelReport json.RawMessage `json:"modelReport"`
}

type rpcRetryFallback struct {
	Role             string `json:"role"`
	OriginalSelector string `json:"originalSelector"`

	Reason string `json:"reason"`
}

type modelSwitch struct {
	From       string `json:"from"`
	To         string `json:"to"`
	EffortFrom string `json:"effortFrom,omitempty"`
	EffortTo   string `json:"effortTo,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Kind       string `json:"kind,omitempty"`
}

type rpcModelReport struct {
	Requested        string        `json:"requested"`
	Ran              string        `json:"ran"`
	Effort           string        `json:"effort"`
	EffortRequested  string        `json:"effortRequested"`
	Switches         []modelSwitch `json:"switches"`
	PolicyViolations []string      `json:"policyViolations"`
}

func rawString(raw json.RawMessage) string {
	var s string
	if len(raw) == 0 || json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}

const maxModelsUsed = 16

const (
	rpcAckTimeout   = 15 * time.Second
	rpcReadyTimeout = 90 * time.Second
	rpcFrameMax     = 8 * 1024 * 1024

	rpcGetStateMaxFailures = 5

	rpcAwaitIdlePolls = 4
)

var (
	rpcSettleTimeout = 60 * time.Second

	rpcIdleTimeout = 15 * time.Minute

	rpcSendGrace = 5 * time.Second

	rpcPollInterval = 500 * time.Millisecond
)

func startRPCChild(store *JobStore, job *Job, stdin io.WriteCloser, stdout io.ReadCloser, logFile *os.File, prompt string) *rpcChild {
	if job.captureDone == nil {
		job.captureDone = make(chan struct{})
	}
	c := &rpcChild{
		stdin:        stdin,
		pending:      make(map[string]chan rpcFrame),
		ready:        make(chan struct{}),
		terminal:     make(chan struct{}, 8),
		job:          job,
		store:        store,
		logFile:      logFile,
		replies:      make(chan []byte, 64),
		stateRefresh: make(chan struct{}, 1),
		done:         make(chan struct{}),
		readDone:     make(chan struct{}),
		lastActivity: time.Now(),
	}
	go c.readLoop(stdout)
	go c.replyLoop()
	go c.finisher()
	go func() {
		select {
		case <-c.ready:
		case <-c.done:
			return
		case <-time.After(rpcReadyTimeout):
			log.Printf("job %s: RPC child never sent a ready frame; closing stdin", job.ID)
			c.close()
			return
		}

		if _, err := c.command(map[string]any{
			"type":           "set_event_filter",
			"events":         nil,
			"messageUpdates": "delta",
		}, rpcAckTimeout, false); err != nil {
			log.Printf("job %s: set_event_filter delta unsupported, continuing in full mode: %v", job.ID, err)
		}
		command := map[string]any{"type": "prompt", "message": prompt}
		if job.Source != nil {
			command["source"] = job.Source
		}
		if _, err := c.command(command, rpcAckTimeout, true); err != nil {
			log.Printf("job %s: initial RPC prompt failed: %v", job.ID, err)
			c.close()
			return
		}

		if _, err := c.getState(); err != nil {
			log.Printf("job %s: initial get_state failed: %v", job.ID, err)
		}
	}()
	return c
}

func (c *rpcChild) command(cmd map[string]any, timeout time.Duration, initialPrompt bool) (rpcFrame, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return rpcFrame{}, fmt.Errorf("rpc stdin is closed")
	}
	c.seq++
	id := fmt.Sprintf("sup-%d", c.seq)
	cmd["id"] = id
	if initialPrompt {
		c.initialPromptID = id
	}
	switch cmd["type"] {
	case "prompt", "steer", "follow_up", "abort_and_prompt":
		c.sends++
		now := time.Now()
		c.lastSend = now
		c.lastActivity = now
	}
	line, err := json.Marshal(cmd)
	if err != nil {
		c.mu.Unlock()
		return rpcFrame{}, err
	}
	ch := make(chan rpcFrame, 1)
	c.pending[id] = ch
	c.mu.Unlock()
	werr := c.writeFrame(append(line, '\n'))

	if werr != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return rpcFrame{}, werr
	}

	select {
	case frame, ok := <-ch:
		if !ok {
			return rpcFrame{}, fmt.Errorf("RPC output closed before response")
		}
		if frame.Success != nil && !*frame.Success {
			return frame, fmt.Errorf("%s rejected: %s", frame.Command, frame.Error)
		}
		return frame, nil
	case <-time.After(timeout):
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return rpcFrame{}, fmt.Errorf("timed out waiting for %v response", cmd["type"])
	}
}

func (c *rpcChild) writeFrame(line []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	closed, stdin := c.closed, c.stdin
	c.mu.Unlock()
	if closed || stdin == nil {
		return fmt.Errorf("rpc stdin is closed")
	}
	_, err := stdin.Write(line)
	return err
}

func (c *rpcChild) steer(mode, text string, source *PromptSource) (rpcFrame, error) {
	if mode != "steer" && mode != "follow_up" && mode != "aside" {
		return rpcFrame{}, fmt.Errorf("unsupported mode %q (use steer, aside, or follow_up)", mode)
	}
	behavior := mode
	if mode == "follow_up" {
		behavior = "followUp"
	}
	command := map[string]any{"type": "prompt", "message": text, "streamingBehavior": behavior}
	if source != nil {
		command["source"] = source
	}
	return c.command(command, rpcAckTimeout, false)
}

func (c *rpcChild) getState() (rpcState, error) {
	var st rpcState
	frame, err := c.command(map[string]any{"type": "get_state"}, rpcAckTimeout, false)
	if err != nil {
		return st, err
	}
	if len(frame.Data) == 0 {
		return st, fmt.Errorf("get_state returned no data")
	}
	if err := json.Unmarshal(frame.Data, &st); err != nil {
		return st, err
	}

	return st, nil
}

func (c *rpcChild) sendCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sends
}

func (c *rpcChild) touch() {
	c.mu.Lock()
	c.lastActivity = time.Now()
	c.mu.Unlock()
}

func (c *rpcChild) sinceActivity() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastActivity.IsZero() {
		return 0
	}
	return time.Since(c.lastActivity)
}

func (c *rpcChild) sinceLastSend() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastSend.IsZero() {
		return 0
	}
	return time.Since(c.lastSend)
}

func (c *rpcChild) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *rpcChild) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	stdin := c.stdin
	if c.done != nil {
		close(c.done)
	}
	c.mu.Unlock()
	if stdin != nil {
		_ = stdin.Close()
	}
}

func (c *rpcChild) readLoop(stdout io.ReadCloser) {
	defer stdout.Close()
	if c.readDone != nil {
		defer close(c.readDone)
	}
	var input io.Reader = stdout
	var capture *captureWriter
	if c.logFile != nil {

		if window := captureCoalesceWindow(); window > 0 {
			capture = newCaptureWriter(c.logFile, window)
			input = io.TeeReader(stdout, capture)
		} else {
			input = io.TeeReader(stdout, c.logFile)
		}
	}

	scanner := newRPCLogScanner(input)
	for scanner.Scan() {
		line := scanner.Bytes()
		frame, err := decodeFrame(line)
		if err != nil {
			log.Printf("job %s: dropping unparsable RPC frame (%d bytes): %v", c.job.ID, len(line), err)
			continue
		}
		c.handleFrame(frame)
	}
	captureErr := scanner.Err()
	if capture != nil {

		if err := capture.Close(); err != nil && captureErr == nil {
			captureErr = err
		}
	}
	if err := captureErr; err != nil {
		log.Printf("job %s: RPC stream read failed: %v", c.job.ID, err)
		c.job.mu.Lock()
		c.job.CaptureError = sanitizeSecretString(err.Error())
		c.job.mu.Unlock()
		if c.store != nil {
			c.store.save(c.job)
		}
		c.close()
	}
	if c.job.captureDone != nil {

		close(c.job.captureDone)
	}

	c.mu.Lock()
	for id, ch := range c.pending {
		close(ch)
		delete(c.pending, id)
	}
	c.mu.Unlock()
}

func (c *rpcChild) handleFrame(frame rpcFrame) {
	c.job.mu.Lock()
	metadataChanged := c.job.observeExecutionLocked(frame)
	c.job.mu.Unlock()
	if metadataChanged && c.store != nil {
		c.store.save(c.job)
	}
	if frame.Type == "response" && frame.Command == "prompt" && frame.Success != nil && !*frame.Success {
		c.mu.Lock()
		initialFailure := frame.ID != "" && frame.ID == c.initialPromptID
		if initialFailure {
			c.terminalError = sanitizeSecretString(string(frame.Error))
			if c.terminalError == "" {
				c.terminalError = "Initial prompt was rejected"
			}
		}
		c.mu.Unlock()
		if initialFailure {
			select {
			case c.terminal <- struct{}{}:
			default:
			}
		}
	}
	if frame.Type == "prompt_result" {
		c.mu.Lock()
		c.promptResultSeen = true
		c.mu.Unlock()
	}

	if frame.Type == "prompt_result" && frame.Status == "error" {
		c.mu.Lock()
		initialFailure := frame.ID != "" && frame.ID == c.initialPromptID
		if initialFailure && c.terminalError == "" {
			c.terminalError = sanitizeSecretString(string(frame.Error))
			if c.terminalError == "" {
				c.terminalError = "Initial prompt failed"
			}
		}
		c.mu.Unlock()
		if initialFailure {
			select {
			case c.terminal <- struct{}{}:
			default:
			}
		}
	}

	if frame.Type != "response" {
		c.touch()
	}
	switch frame.Type {
	case "ready":
		c.readyOne.Do(func() { close(c.ready) })
	case "model_changed":

		select {
		case c.stateRefresh <- struct{}{}:
		default:
		}
	case "response":
		c.mu.Lock()
		ch := c.pending[frame.ID]
		delete(c.pending, frame.ID)
		c.mu.Unlock()
		if ch != nil {
			ch <- frame
		}
	case "tool_execution_start":
		if frame.ToolName != "" {
			c.job.mu.Lock()
			c.job.LastTool = frame.ToolName
			c.job.mu.Unlock()
		}
	case "extension_ui_request":

		switch frame.Method {
		case "confirm", "select", "input", "editor":
			c.reply(map[string]any{"type": "extension_ui_response", "id": frame.ID, "cancelled": true})
		default:

			c.reply(map[string]any{"type": "extension_ui_response", "id": frame.ID, "success": true})
		}
	case "message_end", "turn_end":

		var message rpcMessage
		if json.Unmarshal(frame.Message, &message) == nil {
			c.noteOutcome(message)
		}
		c.noteReport(assistantTextFromMessage(frame.Message))
	case "agent_start":
		c.noteTurn(true)
	case "agent_end":
		if frame.MessageCount > len(frame.Messages) {

			c.noteReportIfEmpty(lastAssistantText(frame.Messages))
		} else {
			for i := len(frame.Messages) - 1; i >= 0; i-- {
				if frame.Messages[i].Role == "assistant" {
					c.noteOutcome(frame.Messages[i])
					break
				}
			}
			c.noteReport(lastAssistantText(frame.Messages))
		}
		if frame.IsTerminal == nil || *frame.IsTerminal {
			c.noteTurn(false)
			select {
			case c.terminal <- struct{}{}:
			default:
			}
		}
	}
}

func (c *rpcChild) reply(payload map[string]any) {
	line, err := json.Marshal(payload)
	if err != nil {
		return
	}
	select {
	case c.replies <- append(line, '\n'):
	case <-c.done:
	default:
		log.Printf("job %s: dropping RPC reply %v; write queue is full", c.job.ID, payload["type"])
	}
}

func (c *rpcChild) replyLoop() {
	for {
		var line []byte
		select {
		case <-c.done:
			return
		case <-c.stateRefresh:
			line = []byte("{\"type\":\"get_state\"}\n")
		case line = <-c.replies:
		}
		if err := c.writeFrame(line); err != nil {
			c.close()
			return
		}
	}
}

func (c *rpcChild) finisher() {
	if !c.awaitFirstTerminal() {
		return
	}
	for {
		if c.waitUntilSettled() {
			return
		}

		if !c.awaitNextTerminal() {
			return
		}
	}
}

func (c *rpcChild) awaitFirstTerminal() bool {
	idlePolls := 0
	failures := 0
	for {
		select {
		case <-c.terminal:
			return true
		case <-c.done:
			return false
		case <-time.After(rpcPollInterval):
		}

		if c.isClosed() {
			return false
		}
		stale := c.sinceActivity()
		if stale < rpcIdleTimeout {

			idlePolls, failures = 0, 0
			continue
		}

		st, err := c.getState()
		if err != nil {
			failures++
			if failures >= rpcGetStateMaxFailures {
				log.Printf("job %s: no terminal turn, silent for %s and get_state unreachable (%v); force-finishing wedged steerable session",
					c.job.ID, stale.Round(time.Second), err)
				c.forceFinish()
				return false
			}
			continue
		}
		failures = 0
		if st.IsStreaming || st.IsCompacting || st.QueuedMessageCount > 0 || c.sinceLastSend() < rpcSendGrace {
			idlePolls = 0
			continue
		}
		idlePolls++
		if idlePolls >= rpcAwaitIdlePolls {
			select {
			case <-c.terminal:
				return true
			default:
			}
			log.Printf("job %s: no terminal agent_end after %s of child silence but the session is idle; finishing",
				c.job.ID, stale.Round(time.Second))
			c.forceFinish()
			return false
		}
	}
}

func (c *rpcChild) awaitNextTerminal() bool {
	idlePolls := 0
	for {

		select {
		case <-c.terminal:
			return true
		case <-c.done:
			return false
		default:
		}

		select {
		case <-c.terminal:
			return true
		case <-c.done:
			return false
		case <-time.After(rpcPollInterval):
		}

		if c.isClosed() {
			return false
		}
		if stale := c.sinceActivity(); stale >= rpcIdleTimeout {
			log.Printf("job %s: no agent_end and no child activity for %s after the last send; force-finishing stalled steerable session",
				c.job.ID, stale.Round(time.Second))
			c.forceFinish()
			return false
		}

		st, err := c.getState()
		if err != nil {

			log.Printf("job %s: get_state while awaiting the next terminal turn failed: %v", c.job.ID, err)
			idlePolls = 0
			continue
		}
		if st.IsStreaming || st.IsCompacting || st.QueuedMessageCount > 0 || c.sinceLastSend() < rpcSendGrace {
			idlePolls = 0
			continue
		}
		idlePolls++
		if idlePolls >= rpcAwaitIdlePolls {
			select {
			case <-c.terminal:
				return true
			default:
			}
			log.Printf("job %s: steered turn emitted no terminal agent_end but the session is idle; finishing", c.job.ID)
			c.forceFinish()
			return false
		}
	}
}

func (c *rpcChild) waitUntilSettled() bool {
	baseline := c.sendCount()
	deadline := time.Now().Add(rpcSettleTimeout)
	failures := 0
	for {
		time.Sleep(rpcPollInterval)
		if c.isClosed() {
			return true
		}
		if c.sendCount() != baseline {

			return false
		}
		st, err := c.getState()
		if err != nil {
			if c.isClosed() {
				return true
			}
			failures++
			log.Printf("job %s: get_state after terminal turn failed (%d): %v", c.job.ID, failures, err)
			if c.sendCount() != baseline {
				return false
			}
			if failures >= rpcGetStateMaxFailures || time.Now().After(deadline) {
				log.Printf("job %s: force-finishing after get_state failures / settle deadline", c.job.ID)
				c.forceFinish()
				return true
			}
			continue
		}
		failures = 0
		if c.sendCount() != baseline {
			return false
		}
		if !st.IsStreaming && !st.IsCompacting && st.QueuedMessageCount == 0 {
			c.forceFinish()
			return true
		}
		if time.Now().After(deadline) {
			log.Printf("job %s: settle deadline expired with streaming=%v compacting=%v queued=%d; force-finishing idle steerable session",
				c.job.ID, st.IsStreaming, st.IsCompacting, st.QueuedMessageCount)
			c.forceFinish()
			return true
		}
	}
}

func (c *rpcChild) noteOutcome(message rpcMessage) {
	if message.Role != "assistant" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.terminalError = ""
	if message.StopReason == "error" || message.StopReason == "aborted" {
		c.terminalError = sanitizeSecretString(message.ErrorMessage)
		if c.terminalError == "" {
			c.terminalError = "Inference ended with " + message.StopReason
		}
	}
}

func (c *rpcChild) forceFinish() {
	c.mu.Lock()
	terminalError := c.terminalError
	c.mu.Unlock()
	c.job.mu.Lock()
	if c.job.Status == "running" {
		c.job.Status = "done"
		c.job.ExitCode = 0
		if terminalError != "" {
			c.job.Status = "error"
			c.job.ExitCode = 1
			c.job.Report = terminalError
		}
		if c.job.EndTime.IsZero() {
			c.job.EndTime = time.Now()
		}
	}
	c.job.mu.Unlock()
	if c.store != nil {
		c.store.save(c.job)
	}
	c.close()
}

func (c *rpcChild) sawPromptResult() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.promptResultSeen
}

const startupStderrTailBytes = 2048

func startupFailureReport(exitErr error, stderrPath string) string {
	report := fmt.Sprintf("omp exited (%v) before reporting a prompt result", exitErr)
	if tail := stderrTail(stderrPath, startupStderrTailBytes); tail != "" {
		report += "; stderr tail:\n" + tail
	}
	return report
}

func stderrTail(path string, limit int) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return ""
	}
	start := info.Size() - int64(limit)
	if start < 0 {
		start = 0
	}
	buf := make([]byte, info.Size()-start)
	n, err := f.ReadAt(buf, start)
	if err != nil && err != io.EOF {
		return ""
	}
	buf = bytes.TrimRight(buf[:n], " \t\r\n")
	if start > 0 {
		if i := bytes.IndexByte(buf, '\n'); i >= 0 {
			buf = buf[i+1:]
		}
	}
	return strings.TrimLeft(string(bytes.ToValidUTF8(buf, nil)), " \t\r\n")
}

func lastAssistantText(messages []rpcMessage) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != "assistant" {
			continue
		}
		var content []rpcContent
		if err := json.Unmarshal(messages[i].Content, &content); err != nil {
			continue
		}
		var parts []string
		for _, part := range content {
			if part.Type == "text" && strings.TrimSpace(part.Text) != "" {
				parts = append(parts, part.Text)
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, "\n")
		}
	}
	return ""
}

func (c *rpcChild) noteReport(text string) {
	if text == "" {
		return
	}
	c.job.mu.Lock()
	c.job.Report = text
	c.job.mu.Unlock()
}

func (c *rpcChild) noteTurn(busy bool) {
	now := time.Now()
	c.job.mu.Lock()
	if busy {
		if !c.job.turnObserved || !c.job.turnBusy {
			c.job.turnBusySince = now
		}
	} else {
		c.job.lastTurnEndedAt = now
	}
	c.job.turnObserved = true
	c.job.turnBusy = busy
	c.job.mu.Unlock()
}

func (c *rpcChild) noteReportIfEmpty(text string) {
	if text == "" {
		return
	}
	c.job.mu.Lock()
	if c.job.Report == "" {
		c.job.Report = text
	}
	c.job.mu.Unlock()
}

func (j *Job) observeExecutionLocked(frame rpcFrame) bool {
	model, thinking, sessionID := "", "", ""
	modelObserved, thinkingObserved := false, false
	var sessionTitle *string
	fallbackFrom, fallbackReason, fallbackSeen := "", "", false
	var report *rpcModelReport
	switch frame.Type {
	case "retry_fallback_applied":

		fallbackSeen = true
		fallbackFrom = rawString(frame.From)
		fallbackReason = rawString(frame.Reason)
	case "message_end":

		var message struct {
			Role       string `json:"role"`
			Provider   string `json:"provider"`
			Model      string `json:"model"`
			StopReason string `json:"stopReason"`
		}
		if json.Unmarshal(frame.Message, &message) != nil || message.Role != "assistant" ||
			message.Provider == "" || message.Model == "" ||
			message.StopReason == "error" || message.StopReason == "aborted" {
			return false
		}
		used := message.Provider + "/" + message.Model
		for _, seen := range j.ModelsUsed {
			if seen == used {
				return false
			}
		}
		if len(j.ModelsUsed) >= maxModelsUsed {
			return false
		}
		j.ModelsUsed = append(j.ModelsUsed, used)
		j.MetadataVersion = metadataVersion
		return true
	case "model_changed":

		modelObserved, thinkingObserved = true, true
	case "thinking_level_changed":
		thinkingObserved = true
		thinking = observedThinkingLevel(frame.ThinkingLevel, frame.Resolved)
	case "response":
		if frame.Success == nil || !*frame.Success {
			return false
		}
		switch frame.Command {
		case "get_state":
			var state rpcState
			if len(frame.Data) == 0 || string(frame.Data) == "null" || json.Unmarshal(frame.Data, &state) != nil {
				return false
			}
			if state.Model != nil && state.Model.Provider != "" && state.Model.ID != "" {
				model = state.Model.Provider + "/" + state.Model.ID
			}
			thinking = observedThinkingLevel(state.ThinkingLevel, "")
			sessionID = state.SessionID
			sessionTitle = state.SessionName
			modelObserved, thinkingObserved = true, true
			var retry rpcRetryFallback
			if len(state.RetryFallback) > 0 && json.Unmarshal(state.RetryFallback, &retry) == nil && retry.OriginalSelector != "" {

				fallbackSeen = true
				if j.FallbackFrom != retry.OriginalSelector {
					j.FallbackFrom = ""
				}
				fallbackFrom = retry.OriginalSelector
				fallbackReason = retry.Reason
			}
			if len(state.ModelReport) > 0 && string(state.ModelReport) != "null" {
				var parsed rpcModelReport
				if json.Unmarshal(state.ModelReport, &parsed) == nil {
					report = &parsed

					if n := len(parsed.Switches); n > 0 && parsed.Switches[n-1].Reason != "" {
						fallbackReason = ""
					}
				}
			}
		case "set_model":

			var selected struct {
				Provider string `json:"provider"`
				ID       string `json:"id"`
			}
			if json.Unmarshal(frame.Data, &selected) != nil || selected.Provider == "" || selected.ID == "" {
				return false
			}
			model = selected.Provider + "/" + selected.ID
			modelObserved = true
			thinkingObserved = model != j.ModelResolved
		default:

			return false
		}
	default:
		return false
	}
	changed := false
	if fallbackSeen {
		if !j.Fallback {
			j.Fallback = true
			changed = true
		}
		if fallbackFrom != "" && j.FallbackFrom == "" {
			j.FallbackFrom = fallbackFrom
			changed = true
		}
		if fallbackReason != "" && fallbackReason != j.FallbackReason {
			j.FallbackReason = fallbackReason
			changed = true
		}
	}
	if report != nil && j.applyModelReportLocked(*report) {
		changed = true
	}
	if modelObserved {
		fallback := j.Fallback

		if model != "" && !j.ModelReportSeen && j.ModelRequested != "" && !isModelRoleSelector(j.ModelRequested) {
			requested, _, _ := parseModelAndThinking(j.ModelRequested, "")
			fallback = fallback || requested != model
		}
		changed = changed || j.ModelResolved != model || j.Model != model || j.Fallback != fallback
		j.ModelResolved, j.Model, j.Fallback = model, model, fallback
	}
	if thinkingObserved && thinking != j.Thinking {
		j.Thinking = thinking
		changed = true
	}
	if sessionID != "" && sessionID != j.SessionID {
		j.SessionID = sessionID
		changed = true
	}
	if sessionTitle != nil && *sessionTitle != j.SessionTitle {
		j.SessionTitle = *sessionTitle
		changed = true
	}
	if changed {
		j.MetadataVersion = metadataVersion
	}
	return changed
}

func (j *Job) applyModelReportLocked(report rpcModelReport) bool {
	changed := !j.ModelReportSeen
	j.ModelReportSeen = true
	switches := report.Switches
	if len(switches) > maxModelsUsed {
		switches = switches[:maxModelsUsed]
	}
	if !slices.Equal(j.ModelSwitches, switches) {
		j.ModelSwitches = append([]modelSwitch(nil), switches...)
		changed = true
	}
	if len(switches) > 0 {
		first := switches[0]
		from := first.From
		if first.EffortFrom != "" {
			from += ":" + first.EffortFrom
		}
		reason := switches[len(switches)-1].Reason
		if !j.Fallback || j.FallbackFrom != from || (reason != "" && j.FallbackReason != reason) {
			changed = true
		}
		j.Fallback, j.FallbackFrom = true, from
		if reason != "" {
			j.FallbackReason = reason
		}
	}
	for _, violation := range report.PolicyViolations {
		if violation != "" && !slices.Contains(j.PolicyViolations, violation) {
			j.PolicyViolations = append(j.PolicyViolations, violation)
			changed = true
		}
	}
	return changed
}

func observedThinkingLevel(level, resolved string) string {
	if resolved != "auto" && knownThinkingLevels[resolved] {
		return resolved
	}
	if level != "auto" && knownThinkingLevels[level] {
		return level
	}
	return ""
}

var rpcRecoveryTailBytes int64 = 4 << 20

func (j *Job) recoverRPCMetadata() {
	if !j.Steerable || j.LogPath == "" {
		return
	}
	capture, err := openCapture(j.LogPath)
	if err != nil {
		return
	}
	defer capture.Close()
	size := capture.Size()
	start := int64(0)
	if size > rpcRecoveryTailBytes {
		start = size - rpcRecoveryTailBytes
	}
	reader, err := capture.ReaderFrom(start)
	if err != nil {
		return
	}
	scanner := newRPCLogScanner(io.LimitReader(reader, size-start))
	var report string

	skipPartial := start > 0
	for scanner.Scan() {
		if skipPartial {
			skipPartial = false
			continue
		}
		frame, err := decodeFrame(scanner.Bytes())
		if err != nil {
			continue
		}
		j.observeExecutionLocked(frame)
		switch frame.Type {
		case "tool_execution_start":
			j.LastTool = frame.ToolName
		case "message_end", "turn_end":
			if text := assistantTextFromMessage(frame.Message); text != "" {
				report = text
			}
		case "agent_end":
			if text := lastAssistantText(frame.Messages); text != "" && (report == "" || frame.MessageCount <= len(frame.Messages)) {
				report = text
			}
		}
	}
	if j.Report == "" {
		j.Report = report
	}
}

func assistantTextFromMessage(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var msg rpcMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return ""
	}
	return lastAssistantText([]rpcMessage{msg})
}

func steerJob(jobID, mode, text string, source *PromptSource) (map[string]any, error) {
	job := globalStore.get(jobID)
	if job == nil {
		return nil, fmt.Errorf("job %q not found", jobID)
	}

	job.mu.RLock()
	status := job.Status
	steerable := job.Steerable
	pid := job.PID
	sessionID := job.SessionID
	child := job.rpc
	job.mu.RUnlock()

	if status == "running" && !isProcessAlive(pid) {
		status = "error"
	}
	if status != "running" {

		return continueFinishedJob(jobID, mode, text, source)
	}
	if child == nil || child.isClosed() {
		if !steerable {
			return nil, fmt.Errorf("job %s runs in print mode (omp -p) and cannot be steered. Start a steerable job with run{\"steerable\": true} (spawns omp --mode rpc), or use schedule to queue a follow-up prompt for after this job finishes", jobID)
		}
		return nil, fmt.Errorf("job %s lost its RPC pipe (the supervisor restarted after the job started), so it cannot be steered. Cancel it and start a fresh run{\"steerable\": true}, or schedule a follow-up", jobID)
	}

	ack, err := child.steer(mode, text, source)
	if err != nil {
		return nil, fmt.Errorf("steer failed: %v", err)
	}

	resp := map[string]any{
		"job_id":     jobID,
		"id":         jobID,
		"delivered":  true,
		"mode":       mode,
		"session_id": sessionID,
		"ack":        ack.Command,
	}
	if st, err := child.getState(); err == nil {
		resp["streaming"] = st.IsStreaming
		resp["queued_messages"] = st.QueuedMessageCount
	}
	return resp, nil
}

func handleSteer(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := request.GetArguments()
	jobID := getJobID(args)
	if jobID == "" {
		if agentRef(args) != "" {
			return handleAgentPrompt(ctx, request, false)
		}
		return mcp.NewToolResultError("job_id is required (agent_id targets a registered live agent instead)"), nil
	}
	source, err := promptSourceArg(args)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	text := firstNonEmptyArg(args, "text", "message", "prompt")
	if text == "" {
		return mcp.NewToolResultError("text is required (message and prompt are accepted aliases)"), nil
	}
	mode, err := normalizeDeliveryMode(getStringArg(args, "mode", "steer"))
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	if sender := strings.TrimSpace(getStringArg(args, "sender_session_id", "")); sender != "" {
		if job := globalStore.get(jobID); job != nil {
			job.mu.RLock()
			target := job.SessionID
			job.mu.RUnlock()
			if target == sender {
				return mcp.NewToolResultError(selfTargetError(jobID, target)), nil
			}
		}
	}
	resp, err := steerJob(jobID, mode, text, source)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return jsonToolResult(resp)
}
