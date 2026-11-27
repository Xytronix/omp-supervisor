package main

import (
	"strings"
	"testing"
)

const creditPromptResultFrame = `{"type":"prompt_result","id":"sup-2","agentInvoked":true,"status":"error","sessionSettled":true,"error":{"message":"400 Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits.","provider":"bifrost","model":"anthropic/claude-opus-5-5","retryable":false,"httpStatus":400}}`

func TestDecodeFrameErrorShapes(t *testing.T) {
	cases := []struct {
		name string
		line string
		want string
	}{
		{"object", creditPromptResultFrame, "400 Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits. (bifrost anthropic/claude-opus-5-5, HTTP 400)"},
		{"string", `{"type":"response","command":"prompt","id":"sup-1","success":false,"error":"No API key found"}`, "No API key found"},
		{"null", `{"type":"response","command":"prompt","id":"sup-1","success":false,"error":null}`, ""},
		{"absent", `{"type":"response","command":"prompt","id":"sup-1","success":true}`, ""},
		{"object without message", `{"type":"prompt_result","id":"sup-2","status":"error","error":{"code":7}}`, `{"code":7}`},
		{"unknown scalar", `{"type":"prompt_result","id":"sup-2","status":"error","error":42}`, "42"},

		{"envelope fallback", `{"type":"prompt_result","id":"sup-2","status":"error","messages":7,"error":{"message":"boom","provider":"p"}}`, "boom (p)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame, err := decodeFrame([]byte(tc.line))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if string(frame.Error) != tc.want {
				t.Fatalf("error = %q, want %q", frame.Error, tc.want)
			}
		})
	}
	frame, err := decodeFrame([]byte(creditPromptResultFrame))
	if err != nil || frame.Type != "prompt_result" || frame.ID != "sup-2" || frame.Status != "error" {
		t.Fatalf("frame = %#v, err = %v", frame, err)
	}
}

func newPromptResultChild(t *testing.T, id string) (*Job, *rpcChild) {
	t.Helper()
	old := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = old })
	job := &Job{ID: id, Status: "running"}
	store.add(job)
	return job, &rpcChild{job: job, store: store, initialPromptID: "sup-2", terminal: make(chan struct{}, 1)}
}

func handleLine(t *testing.T, child *rpcChild, line string) {
	t.Helper()
	frame, err := decodeFrame([]byte(line))
	if err != nil {
		t.Fatalf("decode %s: %v", line, err)
	}
	child.handleFrame(frame)
}

func TestInitialPromptResultErrorFailsJob(t *testing.T) {
	job, child := newPromptResultChild(t, "sub_prompt_result_error")
	handleLine(t, child, `{"type":"response","command":"prompt","id":"sup-2","success":true}`)
	handleLine(t, child, creditPromptResultFrame)
	select {
	case <-child.terminal:
	default:
		t.Fatal("failed prompt_result did not signal a terminal turn")
	}
	child.forceFinish()
	if job.Status != "error" || job.ExitCode == 0 || !strings.Contains(job.Report, "credit balance is too low") || !strings.Contains(job.Report, "HTTP 400") {
		t.Fatalf("job = status %q exit %d report %q", job.Status, job.ExitCode, job.Report)
	}
}

func TestPromptResultErrorKeepsMoreSpecificError(t *testing.T) {
	job, child := newPromptResultChild(t, "sub_prompt_result_specific")
	handleLine(t, child, `{"type":"message_end","message":{"role":"assistant","stopReason":"error","errorMessage":"429 rate limited","content":[]}}`)
	handleLine(t, child, creditPromptResultFrame)
	child.forceFinish()
	if job.Status != "error" || job.Report != "429 rate limited" {
		t.Fatalf("job = status %q report %q", job.Status, job.Report)
	}
}

func TestPromptResultDoesNotFailOtherPrompts(t *testing.T) {
	job, child := newPromptResultChild(t, "sub_prompt_result_ok")
	handleLine(t, child, `{"type":"message_end","message":{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"PONG"}]}}`)
	handleLine(t, child, `{"type":"prompt_result","id":"sup-2","agentInvoked":true,"status":"completed","sessionSettled":true}`)
	handleLine(t, child, strings.Replace(creditPromptResultFrame, `"sup-2"`, `"sup-5"`, 1))
	select {
	case <-child.terminal:
		t.Fatal("non-initial or successful prompt_result signalled a terminal turn")
	default:
	}
	child.forceFinish()
	if job.Status != "done" || job.ExitCode != 0 || job.Report != "PONG" {
		t.Fatalf("job = status %q exit %d report %q", job.Status, job.ExitCode, job.Report)
	}
}

func TestRetryFallbackStateReason(t *testing.T) {
	job := &Job{ID: "sub_startup_reason", Status: "running", ModelRequested: "bifrost/omp-gw/anthropic/claude-opus-5-5"}
	observeLines(t, job,
		`{"type":"response","command":"get_state","success":true,"data":{"model":{"provider":"anthropic","id":"claude-haiku-5-5"},"thinkingLevel":"low","retryFallback":{"role":"default","originalSelector":"bifrost/omp-gw/anthropic/claude-opus-5-5","reason":"Model \"bifrost/omp-gw/anthropic/claude-opus-5-5\" not found"}}}`,
	)
	out := map[string]any{}
	job.addExecutionMetadataLocked(out)
	if out["fallback"] != true || out["fallback_from"] != "bifrost/omp-gw/anthropic/claude-opus-5-5" ||
		out["fallback_reason"] != `Model "bifrost/omp-gw/anthropic/claude-opus-5-5" not found` {
		t.Fatalf("fallback = %v", out)
	}

	observeLines(t, job,
		`{"type":"response","command":"get_state","success":true,"data":{"model":{"provider":"anthropic","id":"claude-haiku-5-5"},"thinkingLevel":"low","retryFallback":{"role":"default","originalSelector":"bifrost/omp-gw/anthropic/claude-opus-5-5"}}}`,
	)
	if job.FallbackReason != `Model "bifrost/omp-gw/anthropic/claude-opus-5-5" not found` {
		t.Fatalf("fallback reason = %q", job.FallbackReason)
	}
}
