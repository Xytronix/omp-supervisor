package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

type replayFrame struct {
	Type    string `json:"type"`
	Message struct {
		Timestamp int64 `json:"timestamp"`
	} `json:"message"`
}

func replayCapture(t *testing.T, input []byte, window, progressStep time.Duration) []byte {
	t.Helper()
	lines := bytes.SplitAfter(input, []byte("\n"))
	stamps := make([]int64, len(lines))
	for i, line := range lines {
		var frame replayFrame
		json.Unmarshal(line, &frame)
		stamps[i] = frame.Message.Timestamp
	}
	if progressStep == 0 {
		prev := -1
		for i := range stamps {
			if stamps[i] == 0 || (prev >= 0 && stamps[i] < stamps[prev]) {
				continue
			}
			if prev >= 0 {
				for k := prev + 1; k < i; k++ {
					stamps[k] = stamps[prev] + (stamps[i]-stamps[prev])*int64(k-prev)/int64(i-prev)
				}
			}
			prev = i
		}
	}
	var out bytes.Buffer
	w, clock := newTestCaptureWriter(&out, window)
	for i, line := range lines {
		if len(line) == 0 {
			continue
		}
		var frame replayFrame
		json.Unmarshal(line, &frame)
		if progressStep > 0 && (frame.Type == "tool_execution_update" || frame.Type == "tool_stream_update") {
			clock.Advance(progressStep)
		} else if stamps[i] > 0 {
			if at := time.UnixMilli(stamps[i]); at.After(clock.Now()) {
				clock.Advance(at.Sub(clock.Now()))
			}
		}
		if _, err := w.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

func checkCoalescedCapture(t *testing.T, input, output []byte) {
	t.Helper()
	type line struct {
		raw       string
		kind, key string
	}
	parse := func(body []byte) []line {
		var lines []line
		for _, raw := range strings.SplitAfter(string(body), "\n") {
			if raw == "" {
				continue
			}
			var f captureFrame
			json.Unmarshal([]byte(raw), &f)
			l := line{raw: raw, kind: f.Type}
			if (f.Type == "tool_execution_update" || f.Type == "tool_stream_update") && f.ToolCallID != "" {
				l.key = progressKey(f.Type, f.ToolCallID)
			}
			lines = append(lines, l)
		}
		return lines
	}
	in, out := parse(input), parse(output)
	if len(output) > 0 && output[len(output)-1] != '\n' && (len(input) == 0 || input[len(input)-1] == '\n') {
		t.Fatal("output ends mid-frame but the input did not")
	}

	var inFixed, outFixed []string
	for _, l := range in {
		if l.key == "" {
			inFixed = append(inFixed, l.raw)
		}
	}
	for _, l := range out {
		if l.key == "" {
			outFixed = append(outFixed, l.raw)
		}
	}
	if strings.Join(inFixed, "") != strings.Join(outFixed, "") {
		t.Fatal("non-progress frames changed or reordered")
	}

	required := map[string]map[int]bool{}
	inByKey := map[string][]string{}
	for _, l := range in {
		if l.key != "" {
			inByKey[l.key] = append(inByKey[l.key], l.raw)
		}
		if l.kind == "tool_execution_end" {
			var f captureFrame
			json.Unmarshal([]byte(l.raw), &f)
			for _, kind := range []string{"tool_execution_update", "tool_stream_update"} {
				key := progressKey(kind, f.ToolCallID)
				if n := len(inByKey[key]); n > 0 {
					if required[key] == nil {
						required[key] = map[int]bool{}
					}
					required[key][n-1] = true
				}
			}
		}
	}
	for key, frames := range inByKey {
		if required[key] == nil {
			required[key] = map[int]bool{}
		}
		required[key][len(frames)-1] = true
	}
	pos := map[string]int{}
	for _, l := range out {
		if l.key == "" {
			continue
		}
		frames := inByKey[l.key]
		i := pos[l.key]
		for i < len(frames) && frames[i] != l.raw {
			if required[l.key][i] {
				t.Fatalf("required snapshot %d of %q dropped", i, l.key)
			}
			i++
		}
		if i == len(frames) {
			t.Fatalf("progress frame of %q not from input or out of order", l.key)
		}
		pos[l.key] = i + 1
	}
	for key, frames := range inByKey {
		if pos[key] != len(frames) {
			t.Fatalf("newest snapshot of %q lost", key)
		}
	}

	lastBeforeEnds := func(lines []line) []string {
		latest := map[string]string{}
		var result []string
		for _, l := range lines {
			if l.key != "" {
				latest[l.key] = l.raw
			}
			if l.kind == "tool_execution_end" {
				var f captureFrame
				json.Unmarshal([]byte(l.raw), &f)
				result = append(result, latest[progressKey("tool_execution_update", f.ToolCallID)], latest[progressKey("tool_stream_update", f.ToolCallID)])
			}
		}
		return result
	}
	inEnds, outEnds := lastBeforeEnds(in), lastBeforeEnds(out)
	if len(inEnds) != len(outEnds) {
		t.Fatal("tool end count changed")
	}
	for i := range inEnds {
		if inEnds[i] != outEnds[i] {
			t.Fatalf("tool end %d preceded by a stale snapshot", i/2)
		}
	}
}

func syntheticTaskCapture() []byte {
	var b strings.Builder
	b.WriteString(`{"type":"ready","protocolVersion":1}` + "\n")
	b.WriteString(`{"type":"agent_start"}` + "\n")
	padding := strings.Repeat("x", 6000)
	ts := int64(1_795_774_102_357)
	for turn := range 4 {
		b.WriteString(fmt.Sprintf(`{"type":"turn_start"}`+"\n"+`{"type":"message_start","message":{"role":"assistant","timestamp":%d}}`+"\n", ts))
		task := fmt.Sprintf("toolu_task_%d", turn)
		bash := fmt.Sprintf("toolu_bash_%d", turn)
		b.WriteString(fmt.Sprintf(`{"type":"tool_execution_start","toolCallId":%q,"toolName":"task","args":{}}`+"\n", task))
		b.WriteString(fmt.Sprintf(`{"type":"tool_execution_start","toolCallId":%q,"toolName":"bash","args":{}}`+"\n", bash))
		for i := range 400 {
			b.WriteString(fmt.Sprintf(`{"type":"tool_execution_update","toolCallId":%q,"toolName":"task","partialResult":{"content":[],"details":{"progress":[{"id":"Worker","status":"running","task":%q,"durationMs":%d}]}}}`+"\n", task, padding, i*150))
			if i%20 == 0 {
				b.WriteString(fmt.Sprintf(`{"type":"tool_execution_update","toolCallId":%q,"toolName":"bash","partialResult":{"content":[{"type":"text","text":"line %d"}]}}`+"\n", bash, i))
			}
			if i == 200 {
				b.WriteString(fmt.Sprintf(`{"type":"tool_execution_end","toolCallId":%q,"toolName":"bash","result":{}}`+"\n", bash))
			}
		}
		b.WriteString(fmt.Sprintf(`{"type":"tool_execution_end","toolCallId":%q,"toolName":"task","result":{}}`+"\n", task))
		ts += 70_000
		b.WriteString(fmt.Sprintf(`{"type":"message_end","message":{"role":"assistant","timestamp":%d}}`+"\n"+`{"type":"turn_end","message":{"role":"assistant","timestamp":%d},"toolResults":[]}`+"\n", ts, ts))
	}
	b.WriteString(`{"type":"agent_end","messages":[]}` + "\n")
	return []byte(b.String())
}

func TestCaptureReplaySyntheticTaskCapture(t *testing.T) {
	input := syntheticTaskCapture()
	output := replayCapture(t, input, time.Second, 150*time.Millisecond)
	checkCoalescedCapture(t, input, output)

	if ratio := float64(len(output)) / float64(len(input)); ratio > 0.2 {
		t.Fatalf("coalesced capture is %.0f%% of the input (%d of %d bytes)", ratio*100, len(output), len(input))
	}
	t.Logf("synthetic capture: %d -> %d bytes", len(input), len(output))
}

func TestCaptureReplayRealCapture(t *testing.T) {
	path := os.Getenv("SUPERVISOR_CAPTURE_REPLAY_FILE")
	if path == "" {
		t.Skip("SUPERVISOR_CAPTURE_REPLAY_FILE not set")
	}
	input, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	steps := map[time.Duration]string{0: "message timestamps interpolated", 150 * time.Millisecond: "150ms per progress frame"}
	for _, step := range []time.Duration{0, 150 * time.Millisecond} {
		output := replayCapture(t, input, time.Second, step)
		checkCoalescedCapture(t, input, output)
		var gz bytes.Buffer
		if _, _, err := writeCaptureGzip(&gz, bytes.NewReader(output)); err != nil {
			t.Fatal(err)
		}
		t.Logf("clock %s: %d -> %d bytes (%.1f%%), indexed gzip %d bytes", steps[step], len(input), len(output),
			100*float64(len(output))/float64(len(input)), gz.Len())
	}
}
