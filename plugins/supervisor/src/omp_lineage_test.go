package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestParentLineageOnlyForFreshSessions(t *testing.T) {
	fresh := buildOmpArgs("p", "", "", "/tmp", false, "", "", false, false, false, "parent-123")
	i := slices.Index(fresh, "--parent-session-id")
	if i < 0 || i+1 >= len(fresh) || fresh[i+1] != "parent-123" {
		t.Fatalf("fresh launch lost parent lineage: %v", fresh)
	}

	resumed := buildOmpArgs("p", "", "", "/tmp", false, "", "child-1", false, false, false, "parent-123")
	if slices.Contains(resumed, "--parent-session-id") || !slices.Contains(resumed, "-r") {
		t.Fatalf("resumed launch rewrote header lineage: %v", resumed)
	}

	continued := buildOmpArgs("p", "", "", "/tmp", false, "", "", true, false, false, "parent-123")
	if slices.Contains(continued, "--parent-session-id") || !slices.Contains(continued, "-c") {
		t.Fatalf("continued launch rewrote header lineage: %v", continued)
	}
}

func TestParentFlagProbeDistinguishesOmpReleases(t *testing.T) {
	dir := t.TempDir()
	script := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}

	old := script("old", "echo 18.12.5")

	current := script("new", "echo 'Error: --parent-session-id requires a raw nonempty OMP session id.' >&2; exit 2")
	if ompParentFlagProbe(old) {
		t.Fatal("release without the flag reported as supporting it")
	}
	if !ompParentFlagProbe(current) {
		t.Fatal("release with the flag reported as unsupported")
	}
	calls := 0
	saved := ompParentFlagProbe
	ompParentFlagProbe = func(string) bool { calls++; return false }
	defer func() { ompParentFlagProbe = saved }()
	ompSupportsParentSessionID(old)
	ompSupportsParentSessionID(old)
	if calls != 1 {
		t.Fatalf("probe ran %d times for an unchanged binary, want 1", calls)
	}
}
