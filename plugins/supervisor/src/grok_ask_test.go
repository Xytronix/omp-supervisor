package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

const testCallbackBase = "https://supervisor.example.test/api/agents/callback/"

func postGrokCallback(t *testing.T, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/callback/"+token, bytes.NewReader([]byte(body)))
	req.SetPathValue("id", token)
	rr := httptest.NewRecorder()
	handleGrokCallback(rr, req)
	return rr
}

func silentGrokBot(t *testing.T, posts *atomic.Int32, tokens chan<- string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		var payload map[string]any
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &payload)
		callbackURL, _ := payload["callback_url"].(string)
		tokens <- strings.TrimPrefix(callbackURL, testCallbackBase)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

const sixGrokTargets = `[
	{"id":"grok-chief-of-staff","name":"Chief of Staff","url":"https://example.test/chief","key":"test-chief"},
	{"id":"grok-coding","name":"Coding","url":"https://example.test/coding","key":"test-coding"},
	{"id":"grok-infra","name":"Infra","url":"https://example.test/infra","key":"test-infra"},
	{"id":"grok-calendar","name":"Calendar","url":"https://example.test/calendar","key":"test-calendar"},
	{"id":"grok-email","name":"Email","url":"https://example.test/email","key":"test-email"},
	{"id":"grok-research","name":"Research","url":"https://example.test/research","key":"test-research"}
]`

func TestResolveGrokPeerRequiresExactConfiguredID(t *testing.T) {
	t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", sixGrokTargets)
	targets, err := loadCompletionWebhookTargets()
	if err != nil || len(targets) != 6 {
		t.Fatalf("six configured routes lost: %d, %v", len(targets), err)
	}
	for _, target := range targets {
		got, err := resolveGrokPeer(target.ID)
		if err != nil || got == nil || *got != target {
			t.Fatalf("%q did not resolve exactly: %#v, %v", target.ID, got, err)
		}
	}
	for _, ref := range []string{"coding", "Coding", "cos", "cos-sub-1", "Chief of Staff", "chief-of-staff", "GROK-CODING", " grok-coding", "grok-coding ", "grok-unknown"} {
		if got, err := resolveGrokPeer(ref); err != nil || got != nil {
			t.Errorf("non-route %q selected a webhook: %#v, %v", ref, got, err)
		}
	}

	t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", `[{"id":"grok-notes","name":"Notes","url":"https://example.test/notes","key":"test-notes"}]`)
	if got, err := resolveGrokPeer("grok-notes"); err != nil || got == nil || got.ID != "grok-notes" {
		t.Fatalf("configured namespace route rejected: %#v, %v", got, err)
	}
}

func TestGrokTargetsDeduplicateAndRejectAmbiguousConfiguration(t *testing.T) {
	t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", `[{"id":"grok-coding","name":"Coding","url":"https://example.test/c","key":"test"},{"id":"grok-coding","name":" Coding ","url":" https://example.test/c ","key":" test "}]`)
	targets, err := loadCompletionWebhookTargets()
	if err != nil || len(targets) != 1 {
		t.Fatalf("duplicate endpoint/key registration survived: %#v, %v", targets, err)
	}
	for _, raw := range []string{
		`null`,
		`[{"id":"grok-coding"`,
		`[{"name":"coding","url":"https://example.test/c","key":"test"}]`,
		`[{"id":"coding","name":"Coding","url":"https://example.test/c","key":"test"}]`,
		`[{"id":"grok-coding","url":"https://example.test/c","key":""}]`,
		`[{"id":"grok-coding","url":"https://example.test/c","key":"test"},{"id":"grok-coding","url":"https://example.test/other","key":"test"}]`,
		`[{"id":"grok-coding","url":"https://example.test/c","key":"test"},{"id":"grok-infra","url":"https://example.test/c","key":"test"}]`,
	} {
		t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", raw)
		if targets, err := loadCompletionWebhookTargets(); err == nil || targets != nil {
			t.Errorf("ambiguous or invalid config accepted: %#v, %v", targets, err)
		}
		if target, err := resolveGrokPeer("grok-coding"); err == nil || target != nil {
			t.Errorf("dispatch accepted partial configuration: %#v, %v", target, err)
		}
	}
}

func TestGrokConfigurationHasNoLegacySingleTargetFallback(t *testing.T) {
	t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", "")
	t.Setenv("SUPERVISOR_WEBHOOK_URL", "https://example.test/legacy")
	t.Setenv("SUPERVISOR_WEBHOOK_NAME", "coding")
	t.Setenv("SUPERVISOR_WEBHOOK_KEY", "test-legacy")
	if targets, err := loadCompletionWebhookTargets(); err != nil || len(targets) != 0 {
		t.Fatalf("legacy settings implicitly registered a webhook: %#v, %v", targets, err)
	}
}

func TestAskGrokPeerPostsCallbackAndWaits(t *testing.T) {
	newTestStores(t)
	t.Setenv("SUPERVISOR_PUBLIC_BASE_URL", "https://supervisor.example.test")

	var gotCallbackURL string
	var gotAsk string
	var posts atomic.Int32
	fakeGrok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("bad json: %v", err)
			w.WriteHeader(500)
			return
		}
		gotAsk, _ = payload["prompt"].(string)
		gotCallbackURL, _ = payload["callback_url"].(string)
		if _, ok := payload["prompt_source"]; ok {
			t.Errorf("invented prompt_source: %#v", payload["prompt_source"])
		}
		if payload["from"] != "omp-test" || payload["source"] != "supervisor" {
			t.Errorf("from/source changed: %#v", payload)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer secret-key" {
			t.Errorf("auth header %q", auth)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))

		go func() {
			time.Sleep(30 * time.Millisecond)
			if rr := postGrokCallback(t, strings.TrimPrefix(gotCallbackURL, testCallbackBase), `{"ok":true,"answer":"pong-from-grok"}`); rr.Code != 200 {
				t.Errorf("callback status %d body %s", rr.Code, rr.Body.String())
			}
		}()
	}))
	defer fakeGrok.Close()

	peer := GrokTarget{ID: "grok-coding", Name: "Coding", URL: fakeGrok.URL, Key: "secret-key"}
	res, err := askGrokPeer(context.Background(), peer, "ping token=xyz", "omp-test", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("askGrokPeer: %v", err)
	}
	if res.Ask.Status != grokAskAnswered || res.Ask.Answer != "pong-from-grok" {
		t.Fatalf("answer %#v", res.Ask)
	}
	if !strings.Contains(gotAsk, "ping token=xyz") {
		t.Fatalf("ask text missing: %q", gotAsk)
	}
	token := strings.TrimPrefix(gotCallbackURL, testCallbackBase)
	if token == gotCallbackURL || token == "" || strings.Contains(res.Ask.ID, token) || res.Ask.TokenHash == token {
		t.Fatalf("callback token %q leaked into the public request %#v", token, res.Ask)
	}
	if posts.Load() != 1 {
		t.Fatalf("expected 1 post, got %d", posts.Load())
	}
	if res.Peer != "grok-coding" || res.Transport != "webhook" {
		t.Fatalf("meta %#v", res)
	}
}

func TestGrokLateAnswerAfterCallerStopsWaitingIsRetrievable(t *testing.T) {
	dataDir := newTestStores(t)
	t.Setenv("SUPERVISOR_PUBLIC_BASE_URL", "https://supervisor.example.test")
	var posts atomic.Int32
	tokens := make(chan string, 4)
	bot := silentGrokBot(t, &posts, tokens)
	t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", fmt.Sprintf(`[{"id":"grok-research","name":"Research","url":%q,"key":"k"},{"id":"grok-coding","name":"Coding","url":%q,"key":"k2"}]`, bot.URL+"/research", bot.URL+"/coding"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"agent_id": "grok-research", "text": "analyze the video", "wait_for": "ack", "wait_seconds": 45}
	result, err := handlePrompt(ctx, req)
	if err != nil || result.IsError {
		t.Fatalf("caller timeout reported as failure: %v %#v", err, result)
	}
	token := <-tokens
	var payload map[string]any
	_ = json.Unmarshal([]byte(result.Content[0].(mcp.TextContent).Text), &payload)
	requestID, _ := payload["request_id"].(string)
	if requestID == "" || payload["status"] != grokAskDelivered || payload["state"] != "pending" || payload["delivered"] != true || payload["note"] == nil {
		t.Fatalf("pending ask lacks a stable request_id/state: %v", payload)
	}
	if strings.Contains(result.Content[0].(mcp.TextContent).Text, token) {
		t.Fatal("prompt result exposed the callback token")
	}
	info, err := os.Stat(filepath.Join(dataDir, "grok-asks", requestID+".json"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("ask record not private: %v %v", info.Mode(), err)
	}

	_, detail := callTool(t, handleAgent, map[string]any{"agent_id": "grok-research"})
	preview, _ := detail["history_preview"].([]any)
	if len(preview) != 1 || preview[0].(map[string]any)["request_id"] != requestID || preview[0].(map[string]any)["state"] != "pending" || detail["pending"] != float64(1) {
		t.Fatalf("open ask not discoverable without its ID: %v", detail)
	}

	reloaded, err := newGrokAskStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	globalGrokAsks = reloaded
	if rr := postGrokCallback(t, token, `{"ok":true,"answer":"late analysis"}`); rr.Code != http.StatusOK {
		t.Fatalf("late callback rejected: %d %s", rr.Code, rr.Body.String())
	}

	reloaded, err = newGrokAskStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	globalGrokAsks = reloaded
	_, got := callTool(t, handleAgent, map[string]any{"agent_id": "grok-research", "request_id": requestID})
	request, _ := got["request"].(map[string]any)
	if request["status"] != grokAskAnswered || request["result"] != "late analysis" {
		t.Fatalf("late answer not retrievable: %v", got)
	}
	_, history := callTool(t, handleAgent, map[string]any{"agent_id": "grok-research", "include_history": true})
	if rows, _ := history["requests"].([]any); len(rows) != 1 || rows[0].(map[string]any)["result"] != "late analysis" || history["acked"] != float64(1) {
		t.Fatalf("history lacks the answer: %v", history)
	}
	if posts.Load() != 1 {
		t.Fatalf("work was resent: %d posts", posts.Load())
	}

	if rr := postGrokCallback(t, token, `{"ok":true,"answer":"late analysis"}`); rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"duplicate":true`) {
		t.Fatalf("duplicate callback: %d %s", rr.Code, rr.Body.String())
	}
	if rr := postGrokCallback(t, token, `{"ok":true,"answer":"rewritten"}`); rr.Code != http.StatusConflict {
		t.Fatalf("conflicting callback: %d %s", rr.Code, rr.Body.String())
	}
	if a, _ := globalGrokAsks.get(requestID); a.Answer != "late analysis" {
		t.Fatalf("first answer overwritten: %q", a.Answer)
	}

	for _, bad := range []string{requestID, "does-not-exist", strings.Repeat("0", len(token))} {
		if rr := postGrokCallback(t, bad, `{"ok":true,"answer":"forged"}`); rr.Code != http.StatusNotFound {
			t.Fatalf("invalid callback %q accepted: %d", bad, rr.Code)
		}
	}
	if res, _ := callTool(t, handleAgent, map[string]any{"agent_id": "grok-coding", "request_id": requestID}); !res.IsError {
		t.Fatal("ask retrieved through a different peer")
	}
}

func TestGrokCallbackBeforeDeliveryBookkeepingKeepsAnswer(t *testing.T) {
	dataDir := newTestStores(t)
	t.Setenv("SUPERVISOR_PUBLIC_BASE_URL", "https://supervisor.example.test")
	bot := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &payload)
		callbackURL, _ := payload["callback_url"].(string)
		if rr := postGrokCallback(t, strings.TrimPrefix(callbackURL, testCallbackBase), `{"answer":"fast"}`); rr.Code != http.StatusOK {
			t.Errorf("early callback rejected: %d", rr.Code)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer bot.Close()
	res, err := askGrokPeer(context.Background(), GrokTarget{ID: "grok-coding", Name: "Coding", URL: bot.URL, Key: "k"}, "hi", "", nil, 0)
	if err != nil || res.Ask.Status != grokAskAnswered || res.Ask.Answer != "fast" {
		t.Fatalf("early answer lost: %#v %v", res, err)
	}
	reloaded, err := newGrokAskStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := reloaded.get(res.Ask.ID); !ok || a.Status != grokAskAnswered || a.Answer != "fast" || !a.DeliveredAt.IsZero() {
		t.Fatalf("persisted state downgraded: %#v", a)
	}
}

func TestGrokCallbackUnstoredAnswerIsRetryable(t *testing.T) {
	newTestStores(t)
	t.Setenv("SUPERVISOR_PUBLIC_BASE_URL", "https://supervisor.example.test")
	var posts atomic.Int32
	tokens := make(chan string, 1)
	bot := silentGrokBot(t, &posts, tokens)
	res, err := askGrokPeer(context.Background(), GrokTarget{ID: "grok-coding", Name: "Coding", URL: bot.URL, Key: "k"}, "hi", "", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	token := <-tokens
	store := globalGrokAsks
	realDir := store.dir
	store.dir = filepath.Join(realDir, "missing")
	if rr := postGrokCallback(t, token, `{"answer":"a"}`); rr.Code != http.StatusInternalServerError {
		t.Fatalf("unstored answer acknowledged: %d", rr.Code)
	}
	if a, _ := store.get(res.Ask.ID); a.Status != grokAskDelivered {
		t.Fatalf("unstored answer changed state: %#v", a)
	}
	store.dir = realDir
	if rr := postGrokCallback(t, token, `{"answer":"a"}`); rr.Code != http.StatusOK {
		t.Fatalf("retry rejected: %d", rr.Code)
	}
}

func TestGrokAskRetentionIsBounded(t *testing.T) {
	dataDir := newTestStores(t)
	store := globalGrokAsks
	_, token, _, err := store.create("grok-coding", "old", "")
	if err != nil {
		t.Fatal(err)
	}
	var oldID string
	for id := range store.asks {
		oldID = id
	}
	store.asks[oldID].CreatedAt = time.Now().Add(-grokAskRetention - time.Minute)
	if rr := postGrokCallback(t, token, `{"answer":"too late"}`); rr.Code != http.StatusNotFound {
		t.Fatalf("expired ask accepted a callback: %d", rr.Code)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "grok-asks", oldID+".json")); !os.IsNotExist(err) {
		t.Fatalf("expired ask file kept: %v", err)
	}

	tokens := make([]string, 0, grokAskMaxRecords)
	openIDs := make([]string, 0, grokAskMaxRecords)
	for i := range grokAskMaxRecords {
		a, tok, _, err := store.create("grok-coding", fmt.Sprint(i), "")
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, tok)
		openIDs = append(openIDs, a.ID)
	}
	var posts atomic.Int32
	bot := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { posts.Add(1) }))
	defer bot.Close()
	if _, err := askGrokPeer(context.Background(), GrokTarget{ID: "grok-coding", Name: "Coding", URL: bot.URL, Key: "k"}, "one more", "", nil, 0); err == nil || !strings.Contains(err.Error(), "nothing was sent") || posts.Load() != 0 {
		t.Fatalf("over-capacity ask not refused before dispatch: %v posts=%d", err, posts.Load())
	}
	for _, id := range openIDs {
		if _, ok := store.get(id); !ok {
			t.Fatalf("pending ask %s evicted under pressure", id)
		}
	}

	if rr := postGrokCallback(t, tokens[0], `{"answer":"done"}`); rr.Code != http.StatusOK {
		t.Fatalf("callback: %d", rr.Code)
	}
	if _, _, _, err := store.create("grok-coding", "admitted", ""); err != nil {
		t.Fatalf("settled ask did not free room: %v", err)
	}
	if _, ok := store.get(openIDs[0]); ok {
		t.Fatal("answered ask kept beyond the cap")
	}
	for _, id := range openIDs[1:] {
		if _, ok := store.get(id); !ok {
			t.Fatalf("pending ask %s evicted instead of the answered one", id)
		}
	}
	files, _ := filepath.Glob(filepath.Join(dataDir, "grok-asks", "*.json"))
	if len(files) != grokAskMaxRecords {
		t.Fatalf("on-disk records not bounded: %d", len(files))
	}
}

func TestGrokWebhookFailureHidesWebhookURL(t *testing.T) {
	newTestStores(t)
	rejecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer rejecting.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()
	for _, tc := range []struct{ base, status string }{{rejecting.URL, grokAskFailed}, {closedURL, grokAskUnconfirmed}} {
		_, err := askGrokPeer(context.Background(), GrokTarget{ID: "grok-coding", Name: "Coding", URL: tc.base + "/hooks/secret-path-id", Key: "k"}, "hi", "", nil, 0)
		if err == nil || strings.Contains(err.Error(), "secret-path-id") || !strings.Contains(err.Error(), "request ask_") {
			t.Fatalf("webhook failure error: %v", err)
		}
		asks := globalGrokAsks.forPeer("grok-coding")
		if a := asks[0]; a.Status != tc.status || !a.DeliveredAt.IsZero() || grokAskState(a.Status) == "answered" || strings.Contains(a.Error, "secret-path-id") {
			t.Fatalf("failed ask record: %#v", a)
		}
	}
}

func TestHandleGrokCallbackUnknown(t *testing.T) {
	newTestStores(t)
	if rr := postGrokCallback(t, "does-not-exist", `{"ok":true,"answer":"x"}`); rr.Code != http.StatusNotFound {
		t.Fatalf("status %d", rr.Code)
	}
}

func TestGrokPromptWaitTimeoutThenRetrieve(t *testing.T) {
	newTestStores(t)
	t.Setenv("SUPERVISOR_PUBLIC_BASE_URL", "https://supervisor.example.test")
	var posts atomic.Int32
	tokens := make(chan string, 1)
	bot := silentGrokBot(t, &posts, tokens)
	t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", fmt.Sprintf(`[{"id":"grok-research","name":"Research","url":%q,"key":"k"}]`, bot.URL))
	start := time.Now()
	result, payload := callTool(t, handlePrompt, map[string]any{"agent_id": "grok-research", "text": "slow work", "wait_for": "ack", "wait_seconds": 1})
	if result.IsError || payload["status"] != grokAskDelivered || payload["state"] != "pending" || payload["delivered"] != true || time.Since(start) < time.Second {
		t.Fatalf("wait timeout not reported as pending: %v", payload)
	}
	if rr := postGrokCallback(t, <-tokens, `{"ok":true,"answer":"finished"}`); rr.Code != http.StatusOK {
		t.Fatalf("late callback: %d", rr.Code)
	}
	_, got := callTool(t, handleAgent, map[string]any{"agent_id": "grok-research", "request_id": payload["request_id"]})
	if req, _ := got["request"].(map[string]any); req["result"] != "finished" || req["state"] != "answered" || posts.Load() != 1 {
		t.Fatalf("answer not retrieved without resend: %v posts=%d", got, posts.Load())
	}
}

func TestGrokPromptPostFailureKeepsRequestMachineReadable(t *testing.T) {
	newTestStores(t)
	t.Setenv("SUPERVISOR_PUBLIC_BASE_URL", "https://supervisor.example.test")
	oldClient := grokWebhookClient
	grokWebhookClient = &http.Client{Timeout: 50 * time.Millisecond}
	t.Cleanup(func() { grokWebhookClient = oldClient })
	tokens := make(chan string, 1)
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &payload)
		callbackURL, _ := payload["callback_url"].(string)
		tokens <- strings.TrimPrefix(callbackURL, testCallbackBase)
		<-release
	}))
	defer slow.Close()
	defer close(release)
	rejecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer rejecting.Close()
	t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", fmt.Sprintf(`[{"id":"grok-research","name":"Research","url":%q,"key":"k"},{"id":"grok-coding","name":"Coding","url":%q,"key":"k2"}]`, slow.URL+"/hook/secret-path-id", rejecting.URL+"/hook/secret-path-id"))

	for _, tc := range []struct{ peer, status, state string }{
		{"grok-research", grokAskUnconfirmed, "pending"},
		{"grok-coding", grokAskFailed, "failed"},
	} {
		result, payload := callTool(t, handlePrompt, map[string]any{"agent_id": tc.peer, "text": "work", "wait_seconds": 1})
		requestID, _ := payload["request_id"].(string)
		errText, _ := payload["error"].(string)
		if !result.IsError || requestID == "" || payload["status"] != tc.status || payload["state"] != tc.state || payload["delivered"] != false || !strings.Contains(errText, requestID) || strings.Contains(payload["__error_text"].(string), "secret-path-id") {
			t.Fatalf("%s: undelivered ask not machine-readable: %v", tc.peer, payload)
		}
		_, detail := callTool(t, handleAgent, map[string]any{"agent_id": tc.peer, "request_id": requestID})
		if req, _ := detail["request"].(map[string]any); req["status"] != tc.status {
			t.Fatalf("%s: stored state differs: %v", tc.peer, detail)
		}
	}

	if rr := postGrokCallback(t, <-tokens, `{"answer":"made it"}`); rr.Code != http.StatusOK {
		t.Fatalf("answer for unconfirmed ask rejected: %d", rr.Code)
	}
}

func TestGrokConcurrentIdenticalCallbacksAndSettle(t *testing.T) {
	newTestStores(t)
	t.Setenv("SUPERVISOR_PUBLIC_BASE_URL", "https://supervisor.example.test")
	const callbacks = 8
	var wg sync.WaitGroup
	codes := make(chan string, callbacks)
	bot := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &payload)
		callbackURL, _ := payload["callback_url"].(string)
		token := strings.TrimPrefix(callbackURL, testCallbackBase)
		for range callbacks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rr := postGrokCallback(t, token, `{"answer":"same"}`)
				codes <- fmt.Sprintf("%d %t", rr.Code, strings.Contains(rr.Body.String(), "duplicate"))
			}()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer bot.Close()
	res, err := askGrokPeer(context.Background(), GrokTarget{ID: "grok-coding", Name: "Coding", URL: bot.URL, Key: "k"}, "hi", "", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(codes)
	seen := map[string]int{}
	for c := range codes {
		seen[c]++
	}
	if seen["200 false"] != 1 || seen["200 true"] != callbacks-1 {
		t.Fatalf("callback outcomes: %v", seen)
	}
	if a, _ := globalGrokAsks.get(res.Ask.ID); a.Status != grokAskAnswered || a.Answer != "same" {
		t.Fatalf("final state: %#v", a)
	}
}

func TestGrokAskReloadReconcilesAndSkipsBadRecords(t *testing.T) {
	dataDir := newTestStores(t)
	a, token, _, err := globalGrokAsks.create("grok-coding", "crashed mid-post", "")
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(dataDir, "grok-asks")
	raw, _ := os.ReadFile(filepath.Join(dir, a.ID+".json"))
	var dup grokAsk
	_ = json.Unmarshal(raw, &dup)
	dup.ID = "ask_dup"
	dupRaw, _ := json.Marshal(dup)
	for name, body := range map[string][]byte{
		"ask_corrupt.json":    []byte("{"),
		"ask_misnamed.json":   raw,
		"ask_dup.json":        dupRaw,
		a.ID + ".json.tmp":    []byte("partial"),
		"ask_other.json.tmp":  []byte("partial"),
		"unrelated-notes.txt": []byte("keep"),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), body, 0600); err != nil {
			t.Fatal(err)
		}
	}
	reloaded, err := newGrokAskStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	globalGrokAsks = reloaded
	if len(reloaded.asks) != 1 || len(reloaded.byHash) != 1 {
		t.Fatalf("bad records loaded: %v", reloaded.asks)
	}
	got, ok := reloaded.get(a.ID)
	if !ok || got.Status != grokAskUnconfirmed || grokAskState(got.Status) != "pending" || got.Error == "" {
		t.Fatalf("crash-left posting not reconciled: %#v", got)
	}
	if again, _ := newGrokAskStore(dataDir); again.asks[a.ID].Status != grokAskUnconfirmed {
		t.Fatal("reconciled state not persisted")
	}
	for _, name := range []string{a.ID + ".json.tmp", "ask_other.json.tmp"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("interrupted write %s kept: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "unrelated-notes.txt")); err != nil {
		t.Fatalf("unrelated file removed: %v", err)
	}
	if rr := postGrokCallback(t, token, `{"answer":"after crash"}`); rr.Code != http.StatusOK {
		t.Fatalf("callback for reconciled ask: %d", rr.Code)
	}
}

func TestListGrokPeerViewsDoesNotInventExecution(t *testing.T) {
	t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", sixGrokTargets)
	views, err := listGrokPeerViews()
	if err != nil || len(views) != 6 {
		t.Fatalf("configured peers lost: %#v, %v", views, err)
	}
	for _, view := range views {
		if view.Source != "grok" || view.Transport != "webhook" || view.Status != "configured" || view.Steerable || len(view.Capabilities) != 1 || view.Capabilities[0] != "prompt" {
			t.Fatalf("webhook advertised as a session: %#v", view)
		}
		raw, err := json.Marshal(view)
		if err != nil {
			t.Fatal(err)
		}
		var row map[string]any
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		for _, absent := range []string{"title", "session_title", "machine", "host", "model", "harness", "last_seen", "last_seen_age_seconds", "queued", "busy", "start_time", "end_time"} {
			if _, exists := row[absent]; exists {
				t.Fatalf("configured peer invented %s: %s", absent, raw)
			}
		}
	}
}
