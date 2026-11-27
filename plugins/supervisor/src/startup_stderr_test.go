package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"
)

const missingModelStderr = "Warning: Permanently added '192.0.2.10' (ED25519) to the list of known hosts.\n" +
	"Model \"bifrost/omp-gw/anthropic/claude-opus-5-5-nonexistent-verify\" not found\n" +
	"\n" +
	"Run `omp models find <pattern>` to search, or `omp models` to list all.\n"

func fakeFailingOmp(t *testing.T, dir, body string) {
	t.Helper()
	path := filepath.Join(dir, "omp")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body+"exit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OMP_PATH", path)
}

func runFailingRPCJob(t *testing.T, body string) *Job {
	t.Helper()
	tempDir := newTestStores(t)
	fakeFailingOmp(t, tempDir, body)
	job, err := startJob(jobSpec{Prompt: "Reply with exactly: OK", CWD: tempDir, Steerable: true})
	if err != nil {
		t.Fatalf("startJob: %v", err)
	}
	waitForJobSettled(t, job.ID)
	<-job.monitorDone
	return job
}

func TestStartupFailureReportsStderrTail(t *testing.T) {
	stderrFixture := filepath.Join(t.TempDir(), "stderr.txt")
	if err := os.WriteFile(stderrFixture, []byte(missingModelStderr), 0o600); err != nil {
		t.Fatal(err)
	}
	var big strings.Builder
	for i := range 5000 {
		big.WriteString("noise line ü ")
		big.WriteString(strings.Repeat("x", i%7))
		big.WriteString("\n")
	}
	big.WriteString("final: Model \"bogus\" not found\n")
	bigFixture := filepath.Join(t.TempDir(), "big.txt")
	if err := os.WriteFile(bigFixture, []byte(big.String()), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		body     string
		lastLine string
		wantAll  string
	}{
		{"no frames", "cat " + stderrFixture + " >&2\n", "Run `omp models find <pattern>` to search, or `omp models` to list all.", strings.TrimSpace(missingModelStderr)},
		{"large stderr", "cat " + bigFixture + " >&2\n", `final: Model "bogus" not found`, ""},
		{"frames without prompt_result", "echo '{\"type\":\"ready\"}'\ncat " + stderrFixture + " >&2\n", "Run `omp models find <pattern>` to search, or `omp models` to list all.", strings.TrimSpace(missingModelStderr)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := runFailingRPCJob(t, tc.body)
			job.mu.RLock()
			status, exitCode, report := job.Status, job.ExitCode, job.Report
			job.mu.RUnlock()
			if status != "error" || exitCode != 1 {
				t.Fatalf("status %q exit %d, want error/1", status, exitCode)
			}
			if !strings.HasPrefix(report, "omp exited (exit status 1)") {
				t.Fatalf("report lacks exit status prefix: %q", report)
			}
			if !strings.HasSuffix(report, tc.lastLine) {
				t.Fatalf("report does not end with the last stderr line: %q", report)
			}
			if tc.wantAll != "" && !strings.Contains(report, tc.wantAll) {
				t.Fatalf("report lost stderr lines: %q", report)
			}
			_, tail, ok := strings.Cut(report, "; stderr tail:\n")
			if !ok {
				t.Fatalf("report has no stderr tail: %q", report)
			}
			if len(tail) > startupStderrTailBytes || !utf8.ValidString(tail) {
				t.Fatalf("tail is %d bytes (bound %d), valid UTF-8 %v", len(tail), startupStderrTailBytes, utf8.ValidString(tail))
			}
			if tc.name == "large stderr" && !strings.HasPrefix(tail, "noise line ü ") {
				t.Fatalf("tail does not start on a whole line: %q", tail[:40])
			}

			if got, _ := job.getReport(8000); !strings.Contains(got, tc.lastLine) {
				t.Fatalf("result report = %q", got)
			}
		})
	}
}

func TestStderrTailBounds(t *testing.T) {
	dir := t.TempDir()
	write := func(content string) string {
		path := filepath.Join(dir, "s")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	if got := stderrTail(filepath.Join(dir, "missing"), 16); got != "" {
		t.Fatalf("missing file tail = %q", got)
	}
	if got := stderrTail(write("one\ntwo\n\n"), 64); got != "one\ntwo" {
		t.Fatalf("small tail = %q", got)
	}
	if got := stderrTail(write("aaaa\nbbbb\ncccc\n"), 12); got != "bbbb\ncccc" {
		t.Fatalf("partial first line kept: %q", got)
	}

	got := stderrTail(write(strings.Repeat("é", 50)), 11)
	if !utf8.ValidString(got) || len(got) > 11 || !strings.HasSuffix(got, "éé") {
		t.Fatalf("long line tail = %q", got)
	}
}
