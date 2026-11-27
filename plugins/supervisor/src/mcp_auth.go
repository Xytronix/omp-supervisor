package main

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

func mcpAuthModeLabel(raw string) string {
	mode := strings.ToLower(strings.TrimSpace(raw))
	if mode == "" {
		return "auto"
	}
	return mode
}

func mcpAuthRequired(mode, host string) (bool, error) {
	switch mcpAuthModeLabel(mode) {
	case "auto":
		return !isLoopbackHost(host), nil
	case "required":
		return true, nil
	case "off":
		return false, nil
	}
	return false, fmt.Errorf("SUPERVISOR_MCP_AUTH=%q: want auto, required, or off", mode)
}

func isLoopbackHost(host string) bool {
	h := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(host), "["), "]")
	if strings.EqualFold(h, "localhost") {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

func requireMCPBearer(auth *registryAuth, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !auth.acceptsEnrollToken(bearerToken(r)) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func gateMCPHandler(next http.Handler, mode, host string, auth *registryAuth) (http.Handler, bool, error) {
	required, err := mcpAuthRequired(mode, host)
	if err != nil || !required {
		return next, false, err
	}
	return requireMCPBearer(auth, next), true, nil
}
