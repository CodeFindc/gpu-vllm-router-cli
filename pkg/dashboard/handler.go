package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Handler serves the web dashboard UI and related topology/probe APIs.
type Handler struct {
	provider TopologyProvider
}

// NewHandler creates a new dashboard HTTP handler.
func NewHandler(provider TopologyProvider) *Handler {
	return &Handler{
		provider: provider,
	}
}

// ServeHTTP handles incoming requests for the dashboard and its REST APIs.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	// CORS headers for API calls
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Session-ID, X-User-ID")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	switch {
	case path == "/dashboard" || path == "/dashboard/" || path == "/ui" || path == "/ui/":
		h.handleDashboardDisabled(w, r)
	case path == "/api/topology":
		h.handleGetTopology(w, r)
	case path == "/api/probe":
		h.handleProbe(w, r)
	case path == "/api/reset-breaker":
		h.handleResetBreaker(w, r)
	case path == "/api/config":
		if r.Method == http.MethodGet {
			h.handleGetConfig(w, r)
		} else if r.Method == http.MethodPost {
			h.handleUpdateConfig(w, r)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case path == "/api/models/rule":
		h.handleUpdateModelRule(w, r)
	case path == "/api/health":
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) handleDashboardDisabled(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"service": "gpu-vllm-router-cli",
		"status":  "active",
		"message": "Web dashboard UI is disabled in CLI mode. Please use CLI commands or REST APIs under /api/.",
	})
}

func (h *Handler) handleGetTopology(w http.ResponseWriter, r *http.Request) {
	if h.provider == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "topology provider not initialized"})
		return
	}

	data, err := h.provider.GetTopology(r.Context())
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("failed to get topology: %v", err)})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(data)
}

func (h *Handler) handleProbe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ProbeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ProbeResponse{
			Success: false,
			Message: "invalid request body: 'url' is required",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if h.provider == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(ProbeResponse{
			URL:     req.URL,
			Success: false,
			Message: "provider not available",
		})
		return
	}

	targetURL := strings.TrimRight(req.URL, "/")
	ok, err := h.provider.ProbeWorker(r.Context(), targetURL)
	msg := "probe succeeded"
	if !ok || err != nil {
		if err != nil {
			msg = err.Error()
		} else {
			msg = "probe failed"
		}
	}

	resp := ProbeResponse{
		URL:     req.URL,
		Success: ok && err == nil,
		Message: msg,
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleResetBreaker(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ResetBreakerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ResetBreakerResponse{
			Success: false,
			Message: "invalid request body: 'url' is required",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if h.provider == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(ResetBreakerResponse{
			URL:     req.URL,
			Success: false,
			Message: "provider not available",
		})
		return
	}

	targetURL := strings.TrimRight(req.URL, "/")
	err := h.provider.ResetBreaker(r.Context(), targetURL)
	msg := "circuit breaker reset to CLOSED"
	if err != nil {
		msg = err.Error()
	}

	resp := ResetBreakerResponse{
		URL:     req.URL,
		Success: err == nil,
		Message: msg,
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	mgr, ok := h.provider.(ConfigManager)
	if !ok || mgr == nil {
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "configuration manager not supported by this provider"})
		return
	}

	cfg, err := mgr.GetConfig(r.Context())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(cfg)
}

func (h *Handler) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	mgr, ok := h.provider.(ConfigManager)
	if !ok || mgr == nil {
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(ConfigUpdateResponse{Success: false, Message: "configuration manager not supported"})
		return
	}

	var req ConfigUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ConfigUpdateResponse{Success: false, Message: fmt.Sprintf("invalid json payload: %v", err)})
		return
	}

	updated, err := mgr.UpdateConfig(r.Context(), req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ConfigUpdateResponse{Success: false, Message: err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ConfigUpdateResponse{
		Success: true,
		Message: "配置已成功更新并实时热生效",
		Config:  updated,
	})
}

func (h *Handler) handleUpdateModelRule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	mgr, ok := h.provider.(ConfigManager)
	if !ok || mgr == nil {
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(ConfigUpdateResponse{Success: false, Message: "configuration manager not supported"})
		return
	}

	var req ModelRuleUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ModelName == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ConfigUpdateResponse{Success: false, Message: "invalid payload: model_name is required"})
		return
	}

	updated, err := mgr.UpdateModelRule(r.Context(), req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ConfigUpdateResponse{Success: false, Message: err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(ConfigUpdateResponse{
		Success: true,
		Message: fmt.Sprintf("模型 %q 规则已成功更新并热生效", req.ModelName),
		Config:  updated,
	})
}
