package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMCPAuthModes(t *testing.T) {
	t.Setenv("SUPERVISOR_REGISTRY_TOKEN", testEnrollToken)
	registry, err := newAgentRegistry(t.TempDir())
	if err != nil {
		t.Fatalf("newAgentRegistry: %v", err)
	}
	prev := globalAgents
	globalAgents = registry
	t.Cleanup(func() { globalAgents = prev })

	stub := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	cases := []struct {
		name   string
		mode   string
		host   string
		bearer string
		want   int
	}{
		{"auto loopback v4 open", "", "127.0.0.1", "", http.StatusOK},
		{"auto loopback v6 open", "auto", "::1", "", http.StatusOK},
		{"auto localhost open", "auto", "localhost", "", http.StatusOK},
		{"auto public no bearer", "", "0.0.0.0", "", http.StatusUnauthorized},
		{"auto public right bearer", "auto", "0.0.0.0", testEnrollToken, http.StatusOK},
		{"auto public wrong bearer", "auto", "192.168.1.5", "wrong-token", http.StatusUnauthorized},
		{"required loopback no bearer", "required", "127.0.0.1", "", http.StatusUnauthorized},
		{"required loopback right bearer", "required", "127.0.0.1", testEnrollToken, http.StatusOK},
		{"off public open", "off", "0.0.0.0", "", http.StatusOK},
		{"off public right bearer", "off", "0.0.0.0", testEnrollToken, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler, _, err := gateMCPHandler(stub, tc.mode, tc.host, registry.auth)
			if err != nil {
				t.Fatalf("gateMCPHandler: %v", err)
			}
			srv := httptest.NewServer(newHTTPMux(handler))
			defer srv.Close()

			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/mcp", nil)
			if tc.bearer != "" {
				req.Header.Set("Authorization", "Bearer "+tc.bearer)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("/mcp status = %d, want %d", resp.StatusCode, tc.want)
			}

			health, err := http.Get(srv.URL + "/health")
			if err != nil {
				t.Fatal(err)
			}
			health.Body.Close()
			if health.StatusCode != http.StatusOK {
				t.Fatalf("/health status = %d, want 200", health.StatusCode)
			}
		})
	}
}

func TestMCPAuthRejectsUnknownMode(t *testing.T) {
	if _, err := mcpAuthRequired("maybe", "127.0.0.1"); err == nil {
		t.Fatal("unknown SUPERVISOR_MCP_AUTH value must be an error")
	}
}
