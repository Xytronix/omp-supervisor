package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRemoteMachineRejectsUnknownInsteadOfLaunchingLocally(t *testing.T) {
	t.Setenv("SUPERVISOR_REMOTE_MACHINES_FILE", "")
	if _, err := resolveRemoteMachine("unknown"); err == nil {
		t.Fatal("unknown machine accepted")
	}
	if target, err := resolveRemoteMachine(""); err != nil || target != nil {
		t.Fatalf("local default: %v %v", target, err)
	}
}

func TestRemoteHostCannotBecomeAnSSHOption(t *testing.T) {
	target := &remoteMachine{SSHHost: "-oProxyCommand=evil", Command: "node runner.mjs", Home: "/home/test"}
	if _, err := remoteCommand(context.Background(), target); err == nil {
		t.Fatal("SSH option host accepted")
	}
}

func TestMachinesListsLaunchAliasesWithoutSessionsOrPrivateConfig(t *testing.T) {
	base := t.TempDir()
	t.Setenv("HOME", base)
	t.Setenv("SUPERVISOR_MACHINE", "local-exec")
	config := filepath.Join(base, "machines.json")
	t.Setenv("SUPERVISOR_REMOTE_MACHINES_FILE", config)
	data := `{
		"local-exec":{"sshHost":"shadowed-local-entry"},
		"mac-host":{"sshHost":"private-mac-route","command":"runner --credential=private-value","home":"/Users/operator","description":"Interactive Mac host","hostname":"devbook","platform":"macos","architecture":"aarch64"},
		"worker-linux-01":{"sshHost":"ssh-worker-01","command":"runner","home":"/srv/omp","description":"Linux OMP worker","hostname":"worker-02","platform":"linux","architecture":"amd64"},
		"alias-two":{"sshHost":"ssh-worker-01","command":"runner","home":"C:\\Users\\operator","platform":"unsupported-os","architecture":"unsupported-arch"}
	}`
	if err := os.WriteFile(config, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	oldAgents, oldStore := globalAgents, globalStore
	globalAgents, globalStore = nil, nil
	t.Cleanup(func() { globalAgents, globalStore = oldAgents, oldStore })

	res, payload := callTool(t, handleMachines, nil)
	if res.IsError {
		t.Fatalf("machines failed: %v", payload)
	}
	rows, ok := payload["machines"].([]any)
	if !ok || len(rows) != 4 || asInt(payload["count"]) != 4 {
		t.Fatalf("inventory lost configured aliases with no sessions: %v", payload)
	}
	wantIDs := []string{"local-exec", "mac-host", "worker-linux-01", "alias-two"}
	byID := make(map[string]map[string]any, len(rows))
	for i, item := range rows {
		row, ok := item.(map[string]any)
		if !ok || row["id"] != wantIDs[i] {
			t.Fatalf("unstable or rewritten selector at %d: %v", i, item)
		}
		byID[wantIDs[i]] = row
		capabilities, ok := row["capabilities"].(map[string]any)
		if !ok {
			t.Fatalf("missing capabilities: %v", row)
		}
		launch, launchOK := capabilities["launch"].(map[string]any)
		sessions, sessionsOK := capabilities["sessions"].(map[string]any)
		if !launchOK || !sessionsOK {
			t.Fatalf("missing launch/session contract: %v", capabilities)
		}
		wantTransport := "ssh-rpc"
		if i == 0 {
			wantTransport = "local"
		}
		modes, _ := launch["modes"].([]any)
		if launch["transport"] != wantTransport || launch["steerableJobs"] != true ||
			len(modes) != 3 || modes[0] != "new" || modes[1] != "resume" || modes[2] != "continue" ||
			sessions["discovery"] != "agents" || sessions["requiresRegistration"] != true {
			t.Fatalf("unsupported or incomplete capabilities: %v", capabilities)
		}
	}
	if byID["local-exec"]["default_cwd"] != base ||
		byID["mac-host"]["default_cwd"] != "/Users/operator" ||
		byID["alias-two"]["default_cwd"] != `C:\Users\operator` {
		t.Fatalf("default directories do not match launch semantics: %v", payload)
	}
	mac := byID["mac-host"]
	if mac["hostname"] != "devbook" || mac["platform"] != "darwin" || mac["architecture"] != "arm64" ||
		mac["description"] != "Interactive Mac host" || byID["worker-linux-01"]["architecture"] != "x64" {
		t.Fatalf("declared host metadata not canonical: %v", payload)
	}
	unknown := byID["alias-two"]
	for _, key := range []string{"hostname", "platform", "architecture"} {
		if _, exists := unknown[key]; exists {
			t.Fatalf("invented metadata %s: %v", key, unknown)
		}
	}
	wire, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"sshHost", "command", "private-mac-route", "ssh-worker-01", "private-value"} {
		if strings.Contains(string(wire), private) {
			t.Fatalf("inventory exposes private transport data %q", private)
		}
	}
	if target, err := resolveRemoteMachine("local-exec"); err != nil || target != nil {
		t.Fatalf("advertised local selector is not accepted locally: %v %v", target, err)
	}
	if target, err := resolveRemoteMachine("alias-two"); err != nil || target == nil || target.Home != `C:\Users\operator` {
		t.Fatalf("inventory alias does not resolve unchanged: %v %v", target, err)
	}
}

func TestMachinesDoesNotHideConfiguredInventoryErrors(t *testing.T) {
	base := t.TempDir()
	t.Setenv("HOME", base)
	t.Setenv("SUPERVISOR_MACHINE", "local-exec")
	t.Setenv("SUPERVISOR_REMOTE_MACHINES_FILE", "")
	if res, payload := callTool(t, handleMachines, nil); res.IsError || asInt(payload["count"]) != 1 {
		t.Fatalf("missing optional default file must retain local execution: %v", payload)
	}
	config := filepath.Join(base, "machines.json")
	t.Setenv("SUPERVISOR_REMOTE_MACHINES_FILE", config)
	if res, payload := callTool(t, handleMachines, nil); !res.IsError || payload["machines"] != nil {
		t.Fatalf("missing explicit configuration was fabricated as local-only: %v", payload)
	}
	for _, data := range []string{
		`{"broken":`,
		`{"missing-home":{"sshHost":"remote","command":"runner"}}`,
		`{"ssh-option":{"sshHost":"-oProxyCommand=evil","command":"runner","home":"/home/operator"}}`,
		`{"missing-command":{"sshHost":"remote","home":"/home/operator"}}`,
		`{" padded-alias ":{"sshHost":"remote","command":"runner","home":"/home/operator"}}`,
	} {
		if err := os.WriteFile(config, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		if res, payload := callTool(t, handleMachines, nil); !res.IsError || payload["machines"] != nil {
			t.Fatalf("invalid allowlist advertised launch capabilities: %s: %v", data, payload)
		}
	}
}

func TestMachineMetadataIsBoundedAndLocalDefaultMustExist(t *testing.T) {
	base := t.TempDir()
	t.Setenv("SUPERVISOR_MACHINE", "local-exec")
	t.Setenv("HOME", filepath.Join(base, "missing-home"))
	config := filepath.Join(base, "machines.json")
	t.Setenv("SUPERVISOR_REMOTE_MACHINES_FILE", config)
	data, err := json.Marshal(map[string]any{
		"worker": map[string]any{
			"sshHost": "remote", "command": "runner", "home": "/home/operator",
			"description": "  " + strings.Repeat("é", 1100) + "  ",
			"hostname":    "  " + strings.Repeat("h", 300) + "  ",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, data, 0600); err != nil {
		t.Fatal(err)
	}
	views, err := listMachineViews()
	if err != nil {
		t.Fatal(err)
	}
	if views[0].DefaultCWD != "" {
		t.Fatalf("advertised unusable local default: %q", views[0].DefaultCWD)
	}
	if views[1].Description != strings.Repeat("é", 1024) || views[1].Hostname != strings.Repeat("h", 255) {
		t.Fatalf("metadata not bounded without corrupting UTF-8: %+v", views[1])
	}
}

func TestFailedRPCInferenceDoesNotReportSuccessfulJob(t *testing.T) {
	old := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = old })
	job := &Job{ID: "sub_failure", Status: "running"}
	store.add(job)
	child := &rpcChild{job: job, store: store}
	child.handleFrame(rpcFrame{Type: "message_end", Message: []byte(`{"role":"assistant","stopReason":"error","errorMessage":"402 quota exhausted","content":[]}`)})
	child.forceFinish()
	if job.Status != "error" || job.ExitCode == 0 || !strings.Contains(job.Report, "quota exhausted") {
		t.Fatalf("failure reported as success: %#v", job)
	}
}

func TestSuccessfulRetryClearsEarlierRPCInferenceError(t *testing.T) {
	old := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = old })
	job := &Job{ID: "sub_recovered", Status: "running"}
	store.add(job)
	child := &rpcChild{job: job, store: store}
	child.handleFrame(rpcFrame{Type: "message_end", Message: []byte(`{"role":"assistant","stopReason":"error","errorMessage":"429","content":[]}`)})
	child.handleFrame(rpcFrame{Type: "message_end", Message: []byte(`{"role":"assistant","stopReason":"stop","content":[{"type":"text","text":"recovered"}]}`)})
	child.forceFinish()
	if job.Status != "done" || job.ExitCode != 0 || job.Report != "recovered" {
		t.Fatalf("retry not recovered: %#v", job)
	}
}

func TestDeferredPromptFailureIsNotACompletedJob(t *testing.T) {
	old := globalStore
	store, err := newJobStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	globalStore = store
	t.Cleanup(func() { globalStore = old })
	job := &Job{ID: "sub_deferred_error", Status: "running"}
	store.add(job)
	child := &rpcChild{job: job, store: store, initialPromptID: "initial"}
	failed := false
	child.handleFrame(rpcFrame{Type: "response", Command: "prompt", ID: "initial", Success: &failed, Error: "No API key found for selected provider"})
	child.forceFinish()
	if job.Status != "error" || job.ExitCode == 0 {
		t.Fatal("deferred prompt error reported as success")
	}
}

func TestLocalAndRemoteRPCCaptureSurvivesRestart(t *testing.T) {
	for _, mode := range []string{"local", "remote"} {
		t.Run(mode, func(t *testing.T) {
			oldStore, oldSchedules := globalStore, globalSchedules
			base := t.TempDir()
			store, err := newJobStore(base)
			if err != nil {
				t.Fatal(err)
			}
			globalStore, globalSchedules = store, nil
			t.Cleanup(func() { globalStore, globalSchedules = oldStore, oldSchedules })
			t.Setenv("HOME", base)
			t.Setenv("SUPERVISOR_MACHINE", "local-test-host")
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("SUPERVISOR_TEST_BINARY", binary)
			t.Setenv("SUPERVISOR_TEST_RPC_CHILD", mode)
			t.Setenv("SUPERVISOR_TEST_HOLD_RPC_CHILD", "1")
			wrapper := filepath.Join(base, "ssh")
			if err := os.WriteFile(wrapper, []byte("#!/bin/sh\nexec \"$SUPERVISOR_TEST_BINARY\" -test.run=^TestSupervisorRPCChildProcess$ -- \"$@\"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("OMP_BIN", wrapper)
			t.Setenv("PATH", base+string(os.PathListSeparator)+os.Getenv("PATH"))
			config := filepath.Join(base, "machines.json")
			if err := os.WriteFile(config, []byte(`{"remote-test-host":{"sshHost":"remote-test-host","command":"trusted-rpc-host","home":"/remote/home with spaces"}}`), 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("SUPERVISOR_REMOTE_MACHINES_FILE", config)
			args := map[string]any{
				"prompt": "produce a final answer", "model": "provider/requested:high",
				"steerable": true, "parent_session_id": "parent-session", "root_session_id": "root-session",
				"parent_agent_id": "parent-agent", "parent_tool_call_id": "parent-call",
				"title": "Launch worker",
			}
			wantHost := "local-test-host"
			if mode == "remote" {
				args["machine"] = "remote-test-host"
				wantHost = "remote-test-host"
			}
			res, receipt := callTool(t, handleRun, args)
			if res.IsError {
				t.Fatalf("launch failed: %v", receipt)
			}
			id, _ := receipt["job_id"].(string)
			job := store.get(id)
			if job == nil {
				t.Fatalf("receipt has no durable job: %v", receipt)
			}
			t.Cleanup(func() {
				job.mu.RLock()
				cancel := job.cancelFn
				job.mu.RUnlock()
				if cancel != nil {
					cancel()
				}
				select {
				case <-job.monitorDone:
				case <-time.After(10 * time.Second):
					t.Error("job monitor did not finish after cancellation")
				}
			})
			if receipt["machine"] != wantHost || receipt["parent_tool_call_id"] != "parent-call" || receipt["title"] != "Launch worker" || receipt["start_time"] == nil {
				t.Fatalf("receipt lost resolved destination/linkage: %v", receipt)
			}
			if wantMaxTime := map[string]any{"local": nil, "remote": "6h"}[mode]; receipt["max_time"] != wantMaxTime {
				t.Fatalf("receipt max_time %v, want %v", receipt["max_time"], wantMaxTime)
			}
			if model, ok := receipt["model_resolved"]; ok && model != "actual/observed" {
				t.Fatalf("request advertised as actual model: %v", receipt)
			}
			if thinking, ok := receipt["thinking"]; ok && thinking != "low" {
				t.Fatalf("request advertised as actual effort: %v", receipt)
			}
			job.mu.RLock()
			child := job.rpc
			job.mu.RUnlock()
			if child == nil {
				t.Fatal("held RPC child has no control pipe")
			}
			if _, err := child.getState(); err != nil {
				t.Fatal(err)
			}
			replacement, err := newJobStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}

			globalStore = replacement
			if _, err := child.steer("follow_up", "finish", nil); err != nil {
				t.Fatal(err)
			}
			select {
			case <-job.monitorDone:
			case <-time.After(10 * time.Second):
				t.Fatal("RPC child monitor did not finish")
			}
			if _, err := os.Stat(filepath.Join(replacement.dataDir, id+".json")); !os.IsNotExist(err) {
				t.Fatalf("child wrote into the replacement job store: %v", err)
			}
			savedData, err := os.ReadFile(filepath.Join(store.dataDir, id+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var saved Job
			if err := json.Unmarshal(savedData, &saved); err != nil || saved.Status != "done" || saved.Report != "last frame survived" || saved.SessionTitle != "Observed worker session" || saved.Title != "Launch worker" || saved.StartTime.IsZero() || saved.EndTime.IsZero() {
				t.Fatalf("original owner lost the terminal job: status=%s report=%q err=%v", saved.Status, saved.Report, err)
			}
			globalStore = store
			_, status := callTool(t, handleStatus, map[string]any{"job_id": id})
			if status["status"] != "done" || status["model_resolved"] != "actual/observed" || status["thinking"] != "low" || status["session_id"] != "observed-session" || status["session_title"] != "Observed worker session" || status["title"] != "Launch worker" {
				t.Fatalf("actual execution metadata missing: %v", status)
			}
			reloaded, err := newJobStore(base)
			if err != nil {
				t.Fatal(err)
			}
			globalStore = reloaded
			eventResult, payload := callTool(t, handleEvents, map[string]any{"job_id": id})
			if eventResult.IsError || payload["stream_closed"] != true || payload["machine"] != wantHost || payload["thinking"] != "low" || payload["root_session_id"] != "root-session" || payload["session_title"] != "Observed worker session" || payload["start_time"] != saved.StartTime.Format(time.RFC3339Nano) || payload["end_time"] != saved.EndTime.Format(time.RFC3339Nano) {
				t.Fatalf("recovered execution metadata lost: %v", payload)
			}
			events, _ := payload["events"].([]any)
			var kinds []string
			for _, raw := range events {
				event := raw.(map[string]any)["event"].(map[string]any)
				kinds = append(kinds, event["type"].(string))
			}
			if strings.Join(kinds, ",") != "message_end,agent_end" {
				t.Fatalf("lost final frames or leaked stderr/control frames: %v", kinds)
			}
			_, result := callTool(t, handleResult, map[string]any{"job_id": id})
			if result["report"] != "last frame survived" || result["model_resolved"] != "actual/observed" || result["parent_session_id"] != "parent-session" || result["parent_agent_id"] != "parent-agent" {
				t.Fatalf("recovered result lost final output/linkage: %v", result)
			}
		})
	}
}

func TestSupervisorRPCChildProcess(t *testing.T) {
	mode := os.Getenv("SUPERVISOR_TEST_RPC_CHILD")
	if mode == "" {
		return
	}
	scanner := bufio.NewScanner(os.Stdin)
	if mode == "remote" {
		if !scanner.Scan() {
			os.Exit(2)
		}
		var launch map[string]any
		if json.Unmarshal(scanner.Bytes(), &launch) != nil || launch["cwd"] != "/remote/home with spaces" || launch["model"] != "provider/requested" || launch["thinking"] != "high" || launch["max_time"] != "24h" {
			os.Exit(3)
		}
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.Encode(map[string]any{"type": "ready"})
	for scanner.Scan() {
		var command map[string]any
		if json.Unmarshal(scanner.Bytes(), &command) != nil {
			os.Exit(4)
		}
		if expected := os.Getenv("SUPERVISOR_TEST_EXPECT_SOURCE"); expected != "" && command["type"] == "prompt" {
			source, _ := command["source"].(map[string]any)
			if expected == "human" {
				if _, exists := command["source"]; exists {
					os.Exit(6)
				}
			} else if source["kind"] != "agent" || source["from"] != expected {
				os.Exit(7)
			}
		}
		response := map[string]any{"type": "response", "id": command["id"], "command": command["type"], "success": true}
		if command["type"] == "get_state" {
			response["data"] = map[string]any{
				"model":         map[string]any{"provider": "actual", "id": "observed"},
				"thinkingLevel": "low", "sessionId": "observed-session", "isStreaming": true,
				"sessionName": "Observed worker session",
			}
		}
		encoder.Encode(response)
		if (command["type"] == "prompt" && command["message"] == "finish") || (command["type"] == "get_state" && os.Getenv("SUPERVISOR_TEST_HOLD_RPC_CHILD") == "") {

			os.Stderr.WriteString("{\"type\":\"agent_start\",\"diagnostic\":true}\n")
			os.Stdout.WriteString("{\"type\":\"message_end\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"last frame survived\"}]}}\n{\"type\":\"agent_end\",\"messages\":[]}\n")
			os.Exit(0)
		}
	}
	os.Exit(5)
}
