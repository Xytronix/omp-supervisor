package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
)

type machineModels struct {
	Roles          map[string]string   `json:"roles"`
	FallbackChains map[string][]string `json:"fallback_chains"`

	Catalog []ompModel `json:"-"`
}

const machineModelsTTL = 5 * time.Minute

var (
	machineModelsMu    sync.Mutex
	machineModelsCache = map[string]machineModelsEntry{}

	loadMachineModels = fetchMachineModels
)

type machineModelsEntry struct {
	models  machineModels
	at      time.Time
	catalog bool
}

func modelsForMachine(ctx context.Context, alias string, target *remoteMachine, catalog bool) (machineModels, error) {
	machineModelsMu.Lock()
	entry, ok := machineModelsCache[alias]
	machineModelsMu.Unlock()
	if ok && time.Since(entry.at) < machineModelsTTL && (entry.catalog || !catalog) {
		return entry.models, nil
	}
	models, err := loadMachineModels(ctx, target, catalog)
	if err != nil {
		if ok && (entry.catalog || !catalog) {
			return entry.models, nil
		}
		return machineModels{}, err
	}
	machineModelsMu.Lock()
	machineModelsCache[alias] = machineModelsEntry{models: models, at: time.Now(), catalog: catalog}
	machineModelsMu.Unlock()
	return models, nil
}

func fetchMachineModels(ctx context.Context, target *remoteMachine, catalog bool) (machineModels, error) {
	if target == nil {
		return localMachineModels(catalog)
	}
	return remoteMachineModels(ctx, target, catalog)
}

func localMachineModels(catalog bool) (machineModels, error) {
	roles, err := runOmp("config", "get", "modelRoles")
	if err != nil {
		return machineModels{}, err
	}
	chains, err := runOmp("config", "get", "retry.fallbackChains")
	if err != nil {
		return machineModels{}, err
	}
	out := machineModels{Roles: stringMap(roles), FallbackChains: chainMap(chains)}
	if catalog {
		if out.Catalog, err = loadModelsCatalog(false); err != nil {
			return machineModels{}, err
		}
	}
	return out, nil
}

func remoteMachineModels(ctx context.Context, target *remoteMachine, catalog bool) (machineModels, error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	cmd, err := remoteCommand(ctx, target)
	if err != nil {
		return machineModels{}, err
	}
	request, _ := json.Marshal(map[string]any{"op": "models", "catalog": catalog})
	cmd.Stdin = bytes.NewReader(append(request, '\n'))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	scanner := bufio.NewScanner(&stdout)
	scanner.Buffer(make([]byte, 64*1024), 64*1024*1024)
	for scanner.Scan() {
		var reply struct {
			Type           string          `json:"type"`
			Roles          json.RawMessage `json:"roles"`
			FallbackChains json.RawMessage `json:"fallbackChains"`
			Models         json.RawMessage `json:"models"`
		}
		if json.Unmarshal(scanner.Bytes(), &reply) != nil || reply.Type != "models" {
			continue
		}
		out := machineModels{Roles: stringMap(reply.Roles), FallbackChains: chainMap(reply.FallbackChains)}
		if catalog && len(reply.Models) > 0 {
			out.Catalog = parseOmpModelsJSON([]byte(`{"models":` + string(reply.Models) + `}`))
		}
		return out, nil
	}
	if runErr != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = runErr.Error()
		}
		return machineModels{}, fmt.Errorf("model inventory failed: %s", sanitizeSecretString(msg))
	}
	return machineModels{}, fmt.Errorf("the host's omp-rpc-host.mjs does not answer the models request; deploy the current client")
}

func stringMap(raw []byte) map[string]string {
	var in map[string]any
	out := map[string]string{}
	if json.Unmarshal(raw, &in) != nil {
		return out
	}
	for key, value := range in {
		if s, ok := value.(string); ok && s != "" {
			out[key] = s
		}
	}
	return out
}

func chainMap(raw []byte) map[string][]string {
	var in map[string]any
	out := map[string][]string{}
	if json.Unmarshal(raw, &in) != nil {
		return out
	}
	for key, value := range in {
		var chain []string
		switch v := value.(type) {
		case string:
			chain = []string{v}
		case []any:
			for _, item := range v {
				if s, ok := item.(string); ok && s != "" {
					chain = append(chain, s)
				}
			}
		}
		if len(chain) > 0 {
			out[key] = chain
		}
	}
	return out
}

func modelsMachine(machine string) (string, *remoteMachine, error) {
	if machine == "" {
		var err error
		if machine, err = defaultLaunchMachine(); err != nil {
			return "", nil, err
		}
	}
	if machine == "" {
		machine = localMachineName()
	}
	target, err := resolveRemoteMachine(machine)
	if err != nil {
		return "", nil, err
	}
	return machine, target, nil
}
