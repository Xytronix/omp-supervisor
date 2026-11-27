package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func configureCompletionPeers(t *testing.T) {
	t.Helper()
	t.Setenv("SUPERVISOR_COMPLETION_WEBHOOKS", `[{"id":"grok-chief-of-staff","name":"Chief of Staff","url":"http://127.0.0.1/chief","key":"test-chief"},{"id":"grok-coding","name":"Coding","url":"http://127.0.0.1/coding","key":"test-coding"}]`)
}

func TestCompletionWebhookConsentAndOwner(t *testing.T) {
	configureCompletionPeers(t)
	cases := []struct {
		name string
		job  *Job
		want string
	}{
		{name: "no job"},
		{name: "owner without opt in", job: &Job{Status: "done", ParentAgentID: "grok-coding"}},
		{name: "opt in without owner", job: &Job{Status: "done", NotifyOnCompletion: true}},
		{name: "unknown owner never falls back", job: &Job{Status: "done", NotifyOnCompletion: true, ParentAgentID: "unregistered"}},
		{name: "completed owner only", job: &Job{Status: "done", NotifyOnCompletion: true, ParentAgentID: "grok-coding"}, want: "grok-coding"},
		{name: "failed owner only", job: &Job{Status: "error", NotifyOnCompletion: true, ParentAgentID: "grok-coding"}, want: "grok-coding"},
		{name: "chief owner only", job: &Job{Status: "done", NotifyOnCompletion: true, ParentAgentID: "grok-chief-of-staff"}, want: "grok-chief-of-staff"},
		{name: "legacy owner requires explicit persisted migration", job: &Job{Status: "done", NotifyOnCompletion: true, ParentAgentID: "coding"}},
		{name: "display label is not an owner route", job: &Job{Status: "done", NotifyOnCompletion: true, ParentAgentID: "Chief of Staff"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			target := tc.job.completionWebhook()
			if tc.want == "" {
				if target != nil {
					t.Fatalf("unsolicited completion routed to %s", target.ID)
				}
				return
			}
			if target == nil || target.ID != tc.want {
				t.Fatalf("completion target = %#v, want only %q", target, tc.want)
			}
		})
	}
}

func TestCompletionWebhookWaitsForTerminalTransition(t *testing.T) {
	configureCompletionPeers(t)
	job := &Job{Status: "running", NotifyOnCompletion: true, ParentAgentID: "grok-coding"}
	if target := job.completionWebhook(); target != nil {
		t.Fatal("running job claimed a completion")
	}
	job.Status = "done"
	if target := job.completionWebhook(); target == nil || target.ID != "grok-coding" {
		t.Fatal("terminal transition lost its requested completion")
	}
}

func TestCompletionWebhookClaimsOnceAcrossConcurrentFinalizers(t *testing.T) {
	configureCompletionPeers(t)
	job := &Job{Status: "canceled", NotifyOnCompletion: true, ParentAgentID: "grok-coding"}
	var count atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if job.completionWebhook() != nil {
				count.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := count.Load(); got != 1 {
		t.Fatalf("cancellation/process-exit claimed %d deliveries, want exactly one", got)
	}
}

func TestCompletionOptInRequiresRoutableOrigin(t *testing.T) {
	configureCompletionPeers(t)
	for _, owner := range []string{"", "unregistered", "coding", "cos", "Chief of Staff"} {
		if err := validateCompletionNotification(true, owner); err == nil {
			t.Errorf("opt-in accepted unroutable owner %q", owner)
		}
	}
	if err := validateCompletionNotification(false, "native-parent"); err != nil {
		t.Fatalf("ordinary native job rejected: %v", err)
	}
	if err := validateCompletionNotification(true, "grok-coding"); err != nil {
		t.Fatalf("configured owner rejected: %v", err)
	}
}

func TestCompletionPayloadPreservesObservedTitleAndTimingOnly(t *testing.T) {
	job := &Job{ID: "sub_notice", Status: "done", Title: "Launch name", Report: "Result"}
	payload := buildCompletionPayload(job)
	for _, absent := range []string{"session_title", "start_time", "end_time"} {
		if _, exists := payload[absent]; exists {
			t.Fatalf("completion invented %s: %v", absent, payload)
		}
	}
	start := time.Date(2026, 10, 17, 8, 0, 0, 0, time.UTC)
	end := start.Add(3 * time.Minute)
	job.SessionTitle, job.StartTime, job.EndTime = "Observed title", start, end
	payload = buildCompletionPayload(job)
	if payload["title"] != "Launch name" || payload["session_title"] != "Observed title" || payload["start_time"] != start || payload["end_time"] != end {
		t.Fatalf("completion lost independent title or execution observations: %v", payload)
	}
}
