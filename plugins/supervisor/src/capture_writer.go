package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultCaptureCoalesce = time.Second

func captureCoalesceWindow() time.Duration {
	raw := strings.TrimSpace(os.Getenv("SUPERVISOR_CAPTURE_COALESCE_MS"))
	if raw == "" {
		return defaultCaptureCoalesce
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || n > 60000 {
		log.Printf("SUPERVISOR_CAPTURE_COALESCE_MS %q is outside 0..60000; using %s", raw, defaultCaptureCoalesce)
		return defaultCaptureCoalesce
	}
	return time.Duration(n) * time.Millisecond
}

type captureWriter struct {
	mu       sync.Mutex
	out      io.Writer
	window   time.Duration
	now      func() time.Time
	schedule func(time.Duration, func()) *time.Timer

	partial []byte
	held    map[string]*heldFrame
	order   []string
	last    map[string]time.Time
	timer   *time.Timer
	timerAt time.Time
	err     error
	closed  bool
}

type heldFrame struct {
	line []byte
	due  time.Time
}

func newCaptureWriter(out io.Writer, window time.Duration) *captureWriter {
	return &captureWriter{
		out:      out,
		window:   window,
		now:      time.Now,
		schedule: time.AfterFunc,
		held:     make(map[string]*heldFrame),
		last:     make(map[string]time.Time),
	}
}

type captureFrame struct {
	Type       string `json:"type"`
	ToolCallID string `json:"toolCallId"`
}

func progressKey(kind, toolCallID string) string { return kind + "\x00" + toolCallID }

func (w *captureWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	n := len(p)
	now := w.now()
	for len(p) > 0 && w.err == nil {
		end := bytes.IndexByte(p, '\n')
		if end < 0 {
			w.partial = append(w.partial, p...)
			break
		}
		line := p[:end+1]
		if len(w.partial) > 0 {
			line = append(w.partial, line...)
			w.partial = nil
		}
		w.frameLocked(line, now)
		p = p[end+1:]
	}
	w.flushDueLocked(now)
	w.armLocked()
	return n, w.err
}

func (w *captureWriter) frameLocked(line []byte, now time.Time) {
	var frame captureFrame
	if json.Unmarshal(line, &frame) != nil {
		w.writeLocked(line)
		return
	}
	switch frame.Type {
	case "tool_execution_update", "tool_stream_update":
		if frame.ToolCallID == "" {
			w.writeLocked(line)
			return
		}
		key := progressKey(frame.Type, frame.ToolCallID)
		if held := w.held[key]; held != nil {
			if now.Before(held.due) {
				held.line = append(held.line[:0], line...)
				return
			}

			w.dropLocked(key)
		} else if last, ok := w.last[key]; !ok || now.Sub(last) >= w.window {
			w.writeLocked(line)
			w.last[key] = now
			return
		} else {
			w.held[key] = &heldFrame{line: slices.Clone(line), due: last.Add(w.window)}
			w.order = append(w.order, key)
			return
		}
		w.writeLocked(line)
		w.last[key] = now
	case "tool_execution_start", "tool_execution_end":
		for _, kind := range []string{"tool_execution_update", "tool_stream_update"} {
			key := progressKey(kind, frame.ToolCallID)
			w.flushKeyLocked(key)
			if frame.Type == "tool_execution_end" {
				delete(w.last, key)
			}
		}
		w.writeLocked(line)
	case "agent_end", "agent_pause", "turn_end":
		w.flushAllLocked(now)
		w.writeLocked(line)
	default:
		w.writeLocked(line)
	}
}

func (w *captureWriter) writeLocked(line []byte) {
	if w.err != nil {
		return
	}
	if _, err := w.out.Write(line); err != nil {
		w.err = err
	}
}

func (w *captureWriter) dropLocked(key string) *heldFrame {
	held := w.held[key]
	if held == nil {
		return nil
	}
	delete(w.held, key)
	if i := slices.Index(w.order, key); i >= 0 {
		w.order = slices.Delete(w.order, i, i+1)
	}
	return held
}

func (w *captureWriter) flushKeyLocked(key string) {
	if held := w.dropLocked(key); held != nil {
		w.writeLocked(held.line)
	}
}

func (w *captureWriter) flushAllLocked(now time.Time) {
	for _, key := range w.order {
		w.writeLocked(w.held[key].line)
		w.last[key] = now
	}
	clear(w.held)
	w.order = w.order[:0]
}

func (w *captureWriter) flushDueLocked(now time.Time) {
	kept := w.order[:0]
	for _, key := range w.order {
		held := w.held[key]
		if now.Before(held.due) {
			kept = append(kept, key)
			continue
		}
		w.writeLocked(held.line)
		w.last[key] = now
		delete(w.held, key)
	}
	w.order = kept
}

func (w *captureWriter) armLocked() {
	if w.schedule == nil || w.closed {
		return
	}
	if len(w.order) == 0 || w.err != nil {
		if w.timer != nil {
			w.timer.Stop()
			w.timer = nil
		}
		return
	}
	due := w.held[w.order[0]].due
	for _, key := range w.order[1:] {
		if d := w.held[key].due; d.Before(due) {
			due = d
		}
	}
	if w.timer != nil && w.timerAt.Equal(due) {
		return
	}
	if w.timer != nil {
		w.timer.Stop()
	}
	w.timerAt = due
	w.timer = w.schedule(due.Sub(w.now()), w.tick)
}

func (w *captureWriter) tick() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.err != nil {
		return
	}
	w.timer = nil
	w.flushDueLocked(w.now())
	w.armLocked()
}

func (w *captureWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return w.err
	}
	w.closed = true
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	w.flushAllLocked(w.now())
	if len(w.partial) > 0 {
		w.writeLocked(w.partial)
		w.partial = nil
	}
	return w.err
}
