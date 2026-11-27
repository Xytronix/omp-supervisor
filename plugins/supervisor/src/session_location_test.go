package main

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeSessionFile(t *testing.T, base, slug, id, prompt string) string {
	t.Helper()
	dir := filepath.Join(base, slug)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "2026-11-18T01-00-00-000Z_"+id+".jsonl")
	body := strings.Join([]string{
		`{"type":"session","version":3,"id":"` + id + `","cwd":"/home/test"}`,
		`{"type":"message","message":{"role":"user","content":[{"type":"text","text":"` + prompt + `"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSessionNeverSubstitutesAnotherLocalSession(t *testing.T) {
	newTestStack(t)
	home := t.TempDir()
	base := filepath.Join(home, ".omp", "agent", "sessions")
	t.Setenv("HOME", home)
	t.Setenv("OMP_SESSIONS_DIR", base)
	t.Setenv("SUPERVISOR_MACHINE", "oracle-test")
	t.Setenv("SUPERVISOR_REMOTE_MACHINES_FILE", filepath.Join(home, "missing-machines.json"))
	writeSessionFile(t, base, "-", "01a1ec4d-d572-75b5-8d67-bf5df88748b8", "Deploy omp fork release")

	res, payload := callTool(t, handleSession, map[string]any{"session_id": "01a00000-0000-7000-8000-000000000001"})
	if !res.IsError {
		t.Fatalf("absent session returned a conversation: %v", payload)
	}
	if text, _ := payload["__error_text"].(string); !strings.Contains(text, "01a00000-0000-7000-8000-000000000001") || strings.Contains(text, "Deploy omp") {
		t.Fatalf("error does not name the requested session clearly: %q", text)
	}

	if res, payload := callTool(t, handleSession, map[string]any{"session_id": "01a1ec4d"}); !res.IsError {
		t.Fatalf("id prefix matched another session: %v", payload)
	}

	res, payload = callTool(t, handleSession, map[string]any{"session_id": "omp:01a1ec4d-d572-75b5-8d67-bf5df88748b8"})
	if res.IsError || payload["session_id"] != "01a1ec4d-d572-75b5-8d67-bf5df88748b8" || payload["machine"] != "oracle-test" {
		t.Fatalf("local session read: %v", payload)
	}
}

func TestSessionReadsFromTheMachineThatHoldsIt(t *testing.T) {
	srv := newTestStack(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("OMP_SESSIONS_DIR", filepath.Join(home, "sessions"))
	t.Setenv("SUPERVISOR_MACHINE", "oracle-test")
	writeSessionFile(t, filepath.Join(home, "sessions"), "-", "01a1ec4d-d572-75b5-8d67-bf5df88748b8", "Deploy omp fork release")

	bin := filepath.Join(home, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}

	fake := `#!/bin/sh
read line
printf '%s\n' "$line" > "$FAKE_SSH_LOG"
case "$line" in
 *01a00000-0000-7000-8000-000000000001*) echo '{"type":"session","found":true,"session":{"session_id":"01a00000-0000-7000-8000-000000000001","title":"worker-01 tasks","cwd":"/tmp","mtime":"2026-11-21T00:00:00Z"},"messages":[{"role":"user","text":"own prompt"},{"role":"tool","text":"x"}]}';;
 *01a1daec-7b15-73fd-947d-b8eb8e97fa2a*) echo '{"type":"session","found":true,"session":{"session_id":"01a1ec4d-d572-75b5-8d67-bf5df88748b8"},"messages":[]}';;
 *) echo '{"type":"session","found":false}';;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FAKE_SSH_LOG", filepath.Join(home, "ssh.log"))
	machines := filepath.Join(home, "machines.json")
	if err := os.WriteFile(machines, []byte(`{"worker-01":{"sshHost":"ssh-worker","command":"bun omp-rpc-host.mjs","home":"/home/worker"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SUPERVISOR_REMOTE_MACHINES_FILE", machines)
	for _, sid := range []string{"01a00000-0000-7000-8000-000000000001", "01a1daec-7b15-73fd-947d-b8eb8e97fa2a", "01a16d15-9485-7657-b790-5dd2e3d0b152"} {
		registerTestAgent(t, srv, map[string]any{"machine": "worker-01", "name": "n-" + sid[:8], "session_id": sid, "harness": "omp-tty"})
	}
	registerTestAgent(t, srv, map[string]any{"machine": "laptop", "name": "l", "session_id": "01a1f8fe-8557-77bd-8bd8-5b490e63c213", "harness": "omp-tty"})

	res, payload := callTool(t, handleSession, map[string]any{"session_id": "01a00000-0000-7000-8000-000000000001", "max_messages": 5})
	if res.IsError || payload["session_id"] != "01a00000-0000-7000-8000-000000000001" || payload["machine"] != "worker-01" || payload["title"] != "worker-01 tasks" {
		t.Fatalf("remote session read: %v", payload)
	}
	msgs, _ := payload["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("remote messages must keep user/assistant text only: %v", msgs)
	}
	if raw, _ := os.ReadFile(filepath.Join(home, "ssh.log")); !strings.Contains(string(raw), `"op":"session"`) {
		t.Fatalf("remote request was not a session op: %s", raw)
	}

	cases := map[string]string{
		"01a1daec-7b15-73fd-947d-b8eb8e97fa2a": "host returned session",
		"01a16d15-9485-7657-b790-5dd2e3d0b152": "was not found on machine worker-01",
		"01a1f8fe-8557-77bd-8bd8-5b490e63c213": "laptop",
	}
	for sid, want := range cases {
		res, payload := callTool(t, handleSession, map[string]any{"session_id": sid})
		text, _ := payload["__error_text"].(string)
		if !res.IsError || !strings.Contains(text, want) || strings.Contains(text, "Deploy omp") {
			t.Fatalf("%s: want an error mentioning %q, got %v", sid, want, payload)
		}
	}
}

func TestAgentsReportBusyIdleState(t *testing.T) {
	srv := newTestStack(t)
	id, token := registerTestAgent(t, srv, map[string]any{"machine": "worker-01", "name": "worker", "session_id": "sess-busy", "harness": "omp-tty"})
	registerTestAgent(t, srv, map[string]any{"machine": "worker-01", "name": "silent", "session_id": "sess-silent", "harness": "omp-tty"})
	beat := func(body map[string]any) {
		t.Helper()
		if code, payload := doJSON(t, http.MethodPost, srv.URL+"/api/agents/"+id+"/heartbeat", token, body); code != http.StatusOK {
			t.Fatalf("heartbeat %d: %v", code, payload)
		}
	}
	row := func(name string) map[string]any {
		t.Helper()
		_, payload := callTool(t, handleAgents, map[string]any{})
		for _, r := range agentRows(t, payload) {
			if r["name"] == name {
				return r
			}
		}
		t.Fatalf("no row %s in %v", name, payload)
		return nil
	}

	if r := row("silent"); r["state"] != "unknown" || r["busy"] != nil {
		t.Fatalf("unreported state must be unknown: %v", r)
	}

	beat(map[string]any{"busy": false})
	if r := row("worker"); r["state"] != "idle" || r["busy"] != false || r["last_turn_ended_at"] != nil {
		t.Fatalf("idle row: %v", r)
	}

	beat(map[string]any{"busy": true})
	r := row("worker")
	if r["state"] != "busy" || r["busy"] != true || r["busy_since"] == nil {
		t.Fatalf("busy row: %v", r)
	}
	since := r["busy_since"]
	beat(map[string]any{"busy": true})
	if r := row("worker"); r["busy_since"] != since {
		t.Fatalf("busy_since moved during the same turn: %v -> %v", since, r["busy_since"])
	}

	before := time.Now().Add(-time.Second)
	beat(map[string]any{"busy": false})
	r = row("worker")
	ended, err := time.Parse(time.RFC3339Nano, r["last_turn_ended_at"].(string))
	if r["state"] != "idle" || err != nil || ended.Before(before) || r["busy_since"] != nil || r["last_turn_ended_age_seconds"] == nil {
		t.Fatalf("turn end from heartbeat transition: %v", r)
	}

	beat(map[string]any{"busy": true})
	exact := time.Now().Add(-3 * time.Second).UTC().Truncate(time.Millisecond)
	beat(map[string]any{"busy": false, "turn_ended_at": exact.Format(time.RFC3339Nano)})
	if r := row("worker"); r["last_turn_ended_at"] != exact.Format(time.RFC3339Nano) {
		t.Fatalf("client turn_ended_at not kept: %v want %s", r["last_turn_ended_at"], exact.Format(time.RFC3339Nano))
	}

	_, detail := callTool(t, handleAgent, map[string]any{"agent_id": id})
	if agent, _ := detail["agent"].(map[string]any); agent["state"] != "idle" {
		t.Fatalf("agent detail lacks state: %v", detail)
	}

	reloaded, err := newAgentRegistry(filepath.Dir(globalAgents.dir))
	if err != nil {
		t.Fatal(err)
	}
	if ag := reloaded.get(id); ag == nil || !ag.BusyReported || ag.Busy || !ag.LastTurnEndedAt.Equal(exact) {
		t.Fatalf("turn state not persisted: %+v", ag)
	}
}

func TestJobAgentViewTracksRPCTurns(t *testing.T) {
	job := &Job{ID: "sub_turns", Status: "running", Steerable: true}
	child := &rpcChild{job: job}
	if v := agentViewFromJob(job); v.State != "unknown" || v.Busy != nil {
		t.Fatalf("no turn seen yet: %+v", v)
	}
	child.handleFrame(rpcFrame{Type: "agent_start"})
	if v := agentViewFromJob(job); v.State != "busy" || v.BusySince == nil {
		t.Fatalf("after agent_start: %+v", v)
	}
	notTerminal := false
	child.handleFrame(rpcFrame{Type: "agent_end", IsTerminal: &notTerminal})
	if v := agentViewFromJob(job); v.State != "busy" {
		t.Fatalf("non-terminal agent_end must stay busy: %+v", v)
	}
	child.handleFrame(rpcFrame{Type: "agent_end"})
	v := agentViewFromJob(job)
	if v.State != "idle" || v.LastTurnEndedAt == nil || v.BusySince != nil {
		t.Fatalf("after terminal agent_end: %+v", v)
	}
	job.Status = "done"
	if v := agentViewFromJob(job); v.State != "unknown" {
		t.Fatalf("finished job must not claim idle: %+v", v)
	}
}
