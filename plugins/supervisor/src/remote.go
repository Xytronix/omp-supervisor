package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
)

type remoteMachine struct {
	SSHHost      string `json:"sshHost"`
	Command      string `json:"command"`
	Home         string `json:"home"`
	Description  string `json:"description,omitempty"`
	Hostname     string `json:"hostname,omitempty"`
	Platform     string `json:"platform,omitempty"`
	Architecture string `json:"architecture,omitempty"`

	Default bool `json:"default,omitempty"`
}

func loadRemoteMachines() (map[string]remoteMachine, error) {
	p := os.Getenv("SUPERVISOR_REMOTE_MACHINES_FILE")
	if p == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		p = filepath.Join(home, ".omp", "supervisor", "machines.json")
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("read remote machines: %w", err)
	}
	var targets map[string]remoteMachine
	if err = json.Unmarshal(data, &targets); err != nil {
		return nil, fmt.Errorf("remote machine configuration: %w", err)
	}
	return targets, nil
}

func defaultLaunchMachine() (string, error) {
	targets, err := loadRemoteMachines()
	if err != nil {
		if os.Getenv("SUPERVISOR_REMOTE_MACHINES_FILE") == "" && errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	chosen := ""
	for alias, target := range targets {
		if !target.Default {
			continue
		}
		if chosen != "" {
			return "", fmt.Errorf("remote machines %q and %q are both marked default", chosen, alias)
		}
		chosen = alias
	}
	if chosen == localMachineName() {
		return "", nil
	}
	return chosen, nil
}

func resolveRemoteMachine(machine string) (*remoteMachine, error) {
	if machine == "" {
		var err error
		if machine, err = defaultLaunchMachine(); err != nil {
			return nil, err
		}
	}
	if machine == "" || machine == localMachineName() {
		return nil, nil
	}
	targets, err := loadRemoteMachines()
	if err != nil {
		return nil, err
	}
	target, ok := targets[machine]
	if !ok {
		return nil, fmt.Errorf("remote machine %q is not configured", machine)
	}
	if target.Home == "" {
		return nil, fmt.Errorf("remote machine %q has no configured home", machine)
	}
	return &target, nil
}

func validateRemoteCommand(target *remoteMachine) error {
	if target.SSHHost == "" || strings.HasPrefix(target.SSHHost, "-") || strings.ContainsAny(target.SSHHost, " \t\r\n\x00") || target.Command == "" || strings.ContainsAny(target.Command, "\r\n\x00") {
		return fmt.Errorf("invalid trusted remote machine configuration")
	}
	return nil
}

func remoteCommand(ctx context.Context, target *remoteMachine) (*exec.Cmd, error) {
	if err := validateRemoteCommand(target); err != nil {
		return nil, err
	}

	return exec.CommandContext(ctx, "ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=20", "-o", "ServerAliveCountMax=3", "--", target.SSHHost, target.Command), nil
}

type hostIdentity struct {
	Hostname     string `json:"hostname,omitempty"`
	Platform     string `json:"platform,omitempty"`
	Architecture string `json:"architecture,omitempty"`
}

func boundedMetadata(value string, maxRunes int) string {
	value = strings.TrimSpace(value)
	count := 0
	for index := range value {
		if count == maxRunes {
			return value[:index]
		}
		count++
	}
	return value
}

func canonicalPlatform(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "linux":
		return "linux"
	case "darwin", "macos", "osx":
		return "darwin"
	case "win32", "windows":
		return "win32"
	default:
		return ""
	}
}

func canonicalArchitecture(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "arm64", "aarch64":
		return "arm64"
	case "x64", "amd64", "x86_64":
		return "x64"
	default:
		return ""
	}
}

func normalizedHostIdentity(host *hostIdentity) *hostIdentity {
	if host == nil {
		return nil
	}
	normalized := hostIdentity{
		Hostname:     boundedMetadata(host.Hostname, 255),
		Platform:     canonicalPlatform(host.Platform),
		Architecture: canonicalArchitecture(host.Architecture),
	}
	if normalized == (hostIdentity{}) {
		return nil
	}
	return &normalized
}

type machineCapabilities struct {
	Launch struct {
		Transport     string    `json:"transport"`
		Modes         [3]string `json:"modes"`
		SteerableJobs bool      `json:"steerableJobs"`
	} `json:"launch"`
	Sessions struct {
		Discovery            string `json:"discovery"`
		RequiresRegistration bool   `json:"requiresRegistration"`
	} `json:"sessions"`
}

func launchCapabilities(transport string) machineCapabilities {
	var capabilities machineCapabilities
	capabilities.Launch.Transport = transport
	capabilities.Launch.Modes = [3]string{"new", "resume", "continue"}
	capabilities.Launch.SteerableJobs = true
	capabilities.Sessions.Discovery = "agents"
	capabilities.Sessions.RequiresRegistration = true
	return capabilities
}

type machineView struct {
	ID           string              `json:"id"`
	Description  string              `json:"description,omitempty"`
	Hostname     string              `json:"hostname,omitempty"`
	Platform     string              `json:"platform,omitempty"`
	Architecture string              `json:"architecture,omitempty"`
	DefaultCWD   string              `json:"default_cwd,omitempty"`
	Default      bool                `json:"default,omitempty"`
	Capabilities machineCapabilities `json:"capabilities"`
}

func listMachineViews() ([]machineView, error) {
	hostname, _ := os.Hostname()
	local := machineView{
		ID:           localMachineName(),
		Description:  "Supervisor host",
		Hostname:     boundedMetadata(hostname, 255),
		Platform:     canonicalPlatform(runtime.GOOS),
		Architecture: canonicalArchitecture(runtime.GOARCH),
		Capabilities: launchCapabilities("local"),
	}

	local.DefaultCWD, _ = resolveJobCwd("")
	targets, err := loadRemoteMachines()
	if err != nil {

		if os.Getenv("SUPERVISOR_REMOTE_MACHINES_FILE") == "" && errors.Is(err, os.ErrNotExist) {
			local.Default = true
			return []machineView{local}, nil
		}
		return nil, err
	}
	defaultID, err := defaultLaunchMachine()
	if err != nil {
		return nil, err
	}
	local.Default = defaultID == ""
	views := make([]machineView, 1, len(targets)+1)
	views[0] = local
	aliases := make([]string, 0, len(targets))
	for alias := range targets {
		if alias == local.ID {

			continue
		}
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	for _, alias := range aliases {
		target := targets[alias]
		if alias == "" || strings.TrimSpace(alias) != alias {
			return nil, fmt.Errorf("remote machine %q is not a usable run.machine selector", alias)
		}
		if target.Home == "" {
			return nil, fmt.Errorf("remote machine %q has no configured home", alias)
		}
		if err := validateRemoteCommand(&target); err != nil {
			return nil, fmt.Errorf("remote machine %q: %w", alias, err)
		}
		views = append(views, machineView{
			ID:           alias,
			Description:  boundedMetadata(target.Description, 1024),
			Hostname:     boundedMetadata(target.Hostname, 255),
			Platform:     canonicalPlatform(target.Platform),
			Architecture: canonicalArchitecture(target.Architecture),
			DefaultCWD:   target.Home,
			Default:      alias == defaultID,
			Capabilities: launchCapabilities("ssh-rpc"),
		})
	}
	return views, nil
}

func handleMachines(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	machines, err := listMachineViews()
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	return jsonToolResult(map[string]any{"machines": machines, "count": len(machines)})
}
