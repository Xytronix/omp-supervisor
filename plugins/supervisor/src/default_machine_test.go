package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeMachines(t *testing.T, data string) {
	t.Helper()
	base := t.TempDir()
	t.Setenv("HOME", base)
	t.Setenv("SUPERVISOR_MACHINE", "control")
	config := filepath.Join(base, "machines.json")
	t.Setenv("SUPERVISOR_REMOTE_MACHINES_FILE", config)
	if err := os.WriteFile(config, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestOmittedMachineUsesConfiguredDefault(t *testing.T) {
	writeMachines(t, `{
		"worker-01":{"sshHost":"ssh-worker","command":"runner","home":"/home/worker","default":true},
		"mac":{"sshHost":"mac","command":"runner","home":"/Users/operator"}
	}`)
	if id, err := defaultLaunchMachine(); err != nil || id != "worker-01" {
		t.Fatalf("default = %q, %v; want worker-01", id, err)
	}
	if target, err := resolveRemoteMachine(""); err != nil || target == nil || target.Home != "/home/worker" {
		t.Fatalf("omitted machine did not resolve to the default target: %v %v", target, err)
	}
	if target, err := resolveRemoteMachine("control"); err != nil || target != nil {
		t.Fatalf("explicit Supervisor host must stay local: %v %v", target, err)
	}

	_, payload := callTool(t, handleMachines, nil)
	defaults := []string{}
	for _, item := range payload["machines"].([]any) {
		row := item.(map[string]any)
		if row["default"] == true {
			defaults = append(defaults, row["id"].(string))
		}
	}
	if len(defaults) != 1 || defaults[0] != "worker-01" {
		t.Fatalf("machines must mark exactly worker-01 default, got %v", defaults)
	}
}

func TestDefaultMachineFallbackAndAmbiguity(t *testing.T) {
	writeMachines(t, `{"mac":{"sshHost":"mac","command":"runner","home":"/Users/operator"}}`)
	if target, err := resolveRemoteMachine(""); err != nil || target != nil {
		t.Fatalf("no marked default must run on the Supervisor host: %v %v", target, err)
	}

	writeMachines(t, `{
		"a":{"sshHost":"a","command":"runner","home":"/a","default":true},
		"b":{"sshHost":"b","command":"runner","home":"/b","default":true}
	}`)
	if _, err := resolveRemoteMachine(""); err == nil {
		t.Fatal("two default machines must be rejected")
	}
	if _, err := startJob(jobSpec{Prompt: "ambiguous"}); err == nil {
		t.Fatal("startJob must refuse an ambiguous default instead of launching")
	}
}
