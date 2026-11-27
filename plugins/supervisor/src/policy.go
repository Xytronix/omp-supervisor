package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

type modelPolicy struct {
	Configured bool          `json:"configured"`
	Mode       string        `json:"mode"`
	Select     string        `json:"select,omitempty"`
	Fallback   string        `json:"fallback,omitempty"`
	CLI        string        `json:"cli,omitempty"`
	Models     []policyModel `json:"models"`
	Roles      []policyRole  `json:"roles"`
}

type policyModel struct {
	ID        string   `json:"id"`
	Route     string   `json:"route,omitempty"`
	Efforts   []string `json:"efforts"`
	Available bool     `json:"available"`

	ChainOnly bool `json:"chainOnly,omitempty"`

	Chat *bool `json:"chat,omitempty"`
}

func (m policyModel) nonChat() bool { return m.Chat != nil && !*m.Chat }

type policyRole struct {
	Role     string `json:"role"`
	Model    string `json:"model,omitempty"`
	Route    string `json:"route,omitempty"`
	Effort   string `json:"effort,omitempty"`
	InPolicy bool   `json:"inPolicy"`
	Guide    string `json:"guide,omitempty"`
}

var policyEfforts = []string{"minimal", "low", "medium", "high", "xhigh", "max"}

func (p *modelPolicy) active() bool {
	return p != nil && p.Configured && p.Mode != "" && p.Mode != "off"
}

func (p *modelPolicy) check(rawModel, thinkingArg string) string {
	if !p.active() {
		return ""
	}
	selector := strings.TrimSpace(rawModel)
	if selector == "" {
		selector = defaultModelSelector
	}
	thinking := strings.TrimSpace(thinkingArg)
	if thinking == "auto" {
		thinking = ""
	}
	if isModelRoleSelector(selector) {
		role, suffix, ok := p.findRole(selector)
		if !ok {
			return fmt.Sprintf("role %s is not configured on this machine", selector)
		}
		effort, reason := explicitEffort(suffix, thinking)
		if reason != "" {
			return reason
		}
		if !role.InPolicy {
			return fmt.Sprintf("role %s resolves to %s, which is outside modelPolicy", selector, role.Model)
		}
		model, ok := p.findModel(role.Model)
		if !ok && role.Route != "" {
			model, ok = p.findModel(role.Route)
		}
		if ok && model.nonChat() {
			return nonChatReason(model)
		}
		if ok && model.ChainOnly {
			return chainOnlyReason(model)
		}
		if effort == "" {
			return ""
		}
		if !ok {
			return fmt.Sprintf("role %s resolves to %s, which is outside modelPolicy", selector, role.Model)
		}
		return effortReason(model, effort)
	}
	model, suffix, ok := p.findModelSelector(selector)
	if !ok {
		return fmt.Sprintf("model %s is outside modelPolicy", strings.TrimSuffix(selector, ":"+suffix))
	}
	if model.nonChat() {
		return nonChatReason(model)
	}
	if model.ChainOnly {
		return chainOnlyReason(model)
	}
	effort, reason := explicitEffort(suffix, thinking)
	if reason != "" {
		return reason
	}
	if effort == "" {
		return ""
	}
	return effortReason(model, effort)
}

func chainOnlyReason(model policyModel) string {
	return fmt.Sprintf("model %s is chain-only in modelPolicy (role values and fallbacks only, never a launch's pick)", model.ID)
}

func nonChatReason(model policyModel) string {
	return fmt.Sprintf("model %s is not a chat model in modelPolicy; an agent needs a chat model", model.ID)
}

func explicitEffort(suffix, thinking string) (string, string) {
	if suffix == "auto" {
		suffix = ""
	}
	if suffix != "" && thinking != "" && suffix != thinking {
		return "", fmt.Sprintf("effort suffix %s and thinking %s differ", suffix, thinking)
	}
	if suffix != "" {
		return suffix, ""
	}
	return thinking, ""
}

func effortReason(model policyModel, effort string) string {
	for _, allowed := range model.Efforts {
		if allowed == effort {
			return ""
		}
	}
	if len(model.Efforts) == 0 {
		return fmt.Sprintf("%s runs only at its default effort; %s is not allowed", model.ID, effort)
	}
	return fmt.Sprintf("%s allows %s, not %s", model.ID, strings.Join(model.Efforts, "|"), effort)
}

func splitLevel(selector string) (string, string) {
	if idx := strings.LastIndex(selector, ":"); idx != -1 && knownThinkingLevels[selector[idx+1:]] {
		return selector[:idx], selector[idx+1:]
	}
	return selector, ""
}

func (p *modelPolicy) findRole(selector string) (policyRole, string, bool) {
	name := strings.TrimPrefix(selector, "@")
	for _, role := range p.Roles {
		if role.Role == name {
			return role, "", true
		}
	}
	base, level := splitLevel(selector)
	if level == "" {
		return policyRole{}, "", false
	}
	name = strings.TrimPrefix(base, "@")
	for _, role := range p.Roles {
		if role.Role == name {
			return role, level, true
		}
	}
	return policyRole{}, "", false
}

func (p *modelPolicy) findModel(id string) (policyModel, bool) {
	for _, model := range p.Models {
		if model.ID == id || (model.Route != "" && model.Route == id) {
			return model, true
		}
	}
	return policyModel{}, false
}

func (p *modelPolicy) findModelSelector(selector string) (policyModel, string, bool) {
	if model, ok := p.findModel(selector); ok {
		return model, "", true
	}
	base, level := splitLevel(selector)
	if level == "" {
		return policyModel{}, "", false
	}
	model, ok := p.findModel(base)
	return model, level, ok
}

func (p *modelPolicy) menu() string {
	var b strings.Builder
	b.WriteString("Allowed here:")
	for _, role := range p.Roles {
		if !role.InPolicy || role.Guide == "" {
			continue
		}
		target := role.Model
		if role.Effort != "" {
			target += ":" + role.Effort
		}
		fmt.Fprintf(&b, "\n- @%s %s: %s", role.Role, target, role.Guide)
	}
	var exact []string
	for _, model := range p.Models {
		if model.ChainOnly || model.nonChat() {
			continue
		}
		if len(model.Efforts) == 0 {
			exact = append(exact, model.ID+" (default effort)")
		} else {
			exact = append(exact, fmt.Sprintf("%s (%s)", model.ID, strings.Join(model.Efforts, "|")))
		}
	}
	if len(exact) > 0 {
		b.WriteString("\nExact ids: " + strings.Join(exact, ", "))
	}
	return b.String()
}

const (
	machinePolicyTTL      = 5 * time.Minute
	machinePolicyErrorTTL = time.Minute
	machinePolicyTimeout  = 30 * time.Second
)

var (
	machinePolicyMu    sync.Mutex
	machinePolicyCache = map[string]machinePolicyEntry{}

	loadMachinePolicy = fetchMachinePolicy
)

type machinePolicyEntry struct {
	policy *modelPolicy
	at     time.Time
	failed bool
}

func policyForMachine(alias string, target *remoteMachine) *modelPolicy {
	machinePolicyMu.Lock()
	entry, ok := machinePolicyCache[alias]
	machinePolicyMu.Unlock()
	if ok {
		ttl := machinePolicyTTL
		if entry.failed {
			ttl = machinePolicyErrorTTL
		}
		if time.Since(entry.at) < ttl {
			return entry.policy
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), machinePolicyTimeout)
	defer cancel()
	policy, err := loadMachinePolicy(ctx, target)
	if err != nil {
		log.Printf("modelPolicy: %s: policy unavailable, launch not checked: %v", alias, err)
	}
	machinePolicyMu.Lock()
	machinePolicyCache[alias] = machinePolicyEntry{policy: policy, at: time.Now(), failed: err != nil}
	machinePolicyMu.Unlock()
	return policy
}

func fetchMachinePolicy(ctx context.Context, target *remoteMachine) (*modelPolicy, error) {
	if target == nil {
		raw, err := runOmp("models", "--policy", "--json")
		if err != nil {

			return nil, nil
		}
		return parseModelPolicy(raw), nil
	}
	return remoteMachinePolicy(ctx, target)
}

func parseModelPolicy(raw []byte) *modelPolicy {
	var probe map[string]json.RawMessage
	if json.Unmarshal(raw, &probe) != nil {
		return nil
	}
	if _, ok := probe["configured"]; !ok {
		return nil
	}
	var policy modelPolicy
	if json.Unmarshal(raw, &policy) != nil {
		return nil
	}
	return &policy
}

func remoteMachinePolicy(ctx context.Context, target *remoteMachine) (*modelPolicy, error) {
	cmd, err := remoteCommand(ctx, target)
	if err != nil {
		return nil, err
	}
	request, _ := json.Marshal(map[string]any{"op": "policy", "max_time": "policy"})
	cmd.Stdin = bytes.NewReader(append(request, '\n'))
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	scanner := bufio.NewScanner(&stdout)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var reply struct {
			Type   string          `json:"type"`
			Policy json.RawMessage `json:"policy"`
		}
		if json.Unmarshal(scanner.Bytes(), &reply) != nil || reply.Type != "policy" {
			continue
		}
		if len(reply.Policy) == 0 || string(reply.Policy) == "null" {
			return nil, nil
		}
		return parseModelPolicy(reply.Policy), nil
	}
	if strings.Contains(stderr.String(), "Remote launch rejected") {

		return nil, nil
	}
	if runErr != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = runErr.Error()
		}
		return nil, fmt.Errorf("policy request failed: %s", sanitizeSecretString(msg))
	}
	return nil, nil
}

func checkLaunchPolicy(alias string, target *remoteMachine, rawModel, thinkingArg string) (string, error) {
	policy := policyForMachine(alias, target)
	reason := policy.check(rawModel, thinkingArg)
	if reason == "" {
		return "", nil
	}
	selector := strings.TrimSpace(rawModel)
	if selector == "" {
		selector = defaultModelSelector
	}
	if thinkingArg != "" {
		selector += " (thinking " + thinkingArg + ")"
	}
	if policy.Mode == "enforce" {
		log.Printf("modelPolicy violation: machine=%s selector=%s reason=%s action=refused", alias, selector, reason)
		return "", fmt.Errorf("model %s refused by modelPolicy on %s: %s\n%s", selector, alias, reason, policy.menu())
	}
	log.Printf("modelPolicy violation: machine=%s selector=%s reason=%s action=reported", alias, selector, reason)
	return fmt.Sprintf("%s: %s: %s", policy.Mode, selector, reason), nil
}

func nonEmpty(s string) []string {
	if s == "" {
		return nil
	}
	return []string{s}
}

const ompModelPolicyEnv = "OMP_MODEL_POLICY=enforce"

var modelLineEscaper = strings.NewReplacer(`&`, "&amp;", `"`, "&quot;", `<`, "&lt;", `>`, "&gt;")

func (j *Job) modelLineLocked() string {
	if j.ModelResolved == "" {
		return ""
	}
	requested := j.ModelRequested
	if j.ThinkingRequested != "" && !strings.HasSuffix(requested, ":"+j.ThinkingRequested) {
		requested += ":" + j.ThinkingRequested
	}
	attrs := [][2]string{{"requested", requested}, {"ran", j.ModelResolved}, {"effort", j.Thinking}}
	var fallback []string
	for i, hop := range j.ModelSwitches {
		step := "-> " + hop.To
		if i == 0 {
			step = hop.From + " " + step
		}
		if hop.Reason != "" {
			step += ": " + hop.Reason
		}
		fallback = append(fallback, step)
	}
	if len(fallback) == 0 && j.Fallback {
		step := "-> " + j.ModelResolved
		if j.FallbackFrom != "" {
			step = j.FallbackFrom + " " + step
		}
		if j.FallbackReason != "" {
			step += ": " + j.FallbackReason
		}
		fallback = append(fallback, step)
	}
	attrs = append(attrs, [2]string{"fallback", strings.Join(fallback, "; ")})
	if j.ThinkingRequested != "" && j.ThinkingRequested != j.Thinking {
		attrs = append(attrs, [2]string{"effort-requested", j.ThinkingRequested})
	}
	attrs = append(attrs, [2]string{"policy", strings.Join(j.PolicyViolations, "; ")})
	var b strings.Builder
	b.WriteString("<model")
	for _, attr := range attrs {
		if attr[1] != "" {
			fmt.Fprintf(&b, ` %s="%s"`, attr[0], modelLineEscaper.Replace(attr[1]))
		}
	}
	b.WriteString("/>")
	return b.String()
}
