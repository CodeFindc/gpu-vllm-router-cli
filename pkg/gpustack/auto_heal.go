package gpustack

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gpu-vllm-router/pkg/config"
)

// RestartRecord tracks restart history for an instance to avoid flapping loops.
type RestartRecord struct {
	Attempts    int
	LastRestart time.Time
}

// AutoHealer orchestrates crash log preservation and automated instance restart.
type AutoHealer struct {
	client  *Client
	cfg     config.AutoHealConfig
	mu      sync.Mutex
	records map[int]*RestartRecord
}

// NewAutoHealer creates a new AutoHealer with the given client and configuration.
func NewAutoHealer(client *Client, cfg config.AutoHealConfig) *AutoHealer {
	if cfg.UnhealthyTimeout <= 0 {
		cfg.UnhealthyTimeout = 3 * time.Minute
	}
	if len(cfg.FatalKeywords) == 0 {
		cfg.FatalKeywords = config.DefaultAutoHealConfig().FatalKeywords
	}
	if cfg.CrashLogDir == "" {
		cfg.CrashLogDir = "logs/crashes"
	}
	if cfg.MaxRestartAttempts <= 0 {
		cfg.MaxRestartAttempts = 3
	}
	if cfg.RestartCooldown <= 0 {
		cfg.RestartCooldown = 5 * time.Minute
	}

	return &AutoHealer{
		client:  client,
		cfg:     cfg,
		records: make(map[int]*RestartRecord),
	}
}

// ShouldRestart checks if an unhealthy worker meets the criteria for automated restart.
func (h *AutoHealer) ShouldRestart(openSince time.Time, lastErr string) (bool, string) {
	if !h.cfg.Enabled {
		return false, ""
	}

	// 1. Check fatal keywords (e.g. CUDA OOM, NCCL error, Engine is dead)
	if lastErr != "" {
		for _, kw := range h.cfg.FatalKeywords {
			if strings.Contains(strings.ToLower(lastErr), strings.ToLower(kw)) {
				return true, fmt.Sprintf("Matched fatal error keyword %q in last error: %s", kw, lastErr)
			}
		}
	}

	// 2. Check unhealthy timeout (persistent OPEN state)
	if !openSince.IsZero() && time.Since(openSince) >= h.cfg.UnhealthyTimeout {
		return true, fmt.Sprintf("Unhealthy timeout exceeded: in OPEN state for %v (threshold: %v)",
			time.Since(openSince).Round(time.Second), h.cfg.UnhealthyTimeout)
	}

	return false, ""
}

// TriggerIncidentRestart preserves logs to disk and restarts the model instance.
func (h *AutoHealer) TriggerIncidentRestart(ctx context.Context, ep WorkerEndpoint, triggerReason string, lastErr string) (string, error) {
	if !h.cfg.Enabled || h.client == nil || ep.InstanceID <= 0 {
		return "", nil
	}

	h.mu.Lock()
	rec, exists := h.records[ep.InstanceID]
	if !exists {
		rec = &RestartRecord{}
		h.records[ep.InstanceID] = rec
	}

	// Guard against infinite restart loops
	if !rec.LastRestart.IsZero() && time.Since(rec.LastRestart) < h.cfg.RestartCooldown {
		h.mu.Unlock()
		log.Printf("[AutoHeal] ⚠️ Instance %s (ID: %d) restart skipped: within cooldown (%v remaining)",
			ep.InstanceName, ep.InstanceID, (h.cfg.RestartCooldown - time.Since(rec.LastRestart)).Round(time.Second))
		return "", nil
	}

	if rec.Attempts >= h.cfg.MaxRestartAttempts {
		h.mu.Unlock()
		log.Printf("[AutoHeal] 🚨 CRITICAL: Instance %s (ID: %d) reached maximum restart attempts (%d)! Halting auto-restart to prevent flapping.",
			ep.InstanceName, ep.InstanceID, h.cfg.MaxRestartAttempts)
		return "", fmt.Errorf("instance %d exceeded maximum restart attempts (%d)", ep.InstanceID, h.cfg.MaxRestartAttempts)
	}

	rec.Attempts++
	rec.LastRestart = time.Now()
	h.mu.Unlock()

	log.Printf("[AutoHeal] ⚡ Initiating automated incident mitigation for instance %s (ID: %d, Model: %s, URL: %s)...",
		ep.InstanceName, ep.InstanceID, ep.ModelName, ep.URL)
	log.Printf("  Reason: %s", triggerReason)

	// Step 1: Fetch instance container logs before restart
	instanceLogs, logErr := h.client.GetInstanceLogs(ctx, ep.InstanceID, 1000)
	if logErr != nil {
		log.Printf("[AutoHeal] Warning: failed to fetch container logs for instance %d: %v", ep.InstanceID, logErr)
		instanceLogs = fmt.Sprintf("Failed to fetch logs from GPUStack API: %v", logErr)
	}

	// Step 2: Build structured crash audit report
	nowStr := time.Now().UTC().Format(time.RFC3339)
	var report strings.Builder
	report.WriteString("================================================================================\n")
	report.WriteString("[GPU-VLLM-ROUTER INCIDENT CRASH AUDIT REPORT]\n")
	fmt.Fprintf(&report, "Timestamp:           %s\n", nowStr)
	fmt.Fprintf(&report, "Model Name:          %s\n", ep.ModelName)
	fmt.Fprintf(&report, "Instance ID:         %d\n", ep.InstanceID)
	fmt.Fprintf(&report, "Instance Name:       %s\n", ep.InstanceName)
	fmt.Fprintf(&report, "Worker Node:         %s\n", ep.WorkerName)
	fmt.Fprintf(&report, "Endpoint URL:        %s\n", ep.URL)
	fmt.Fprintf(&report, "Backend Engine:      %s\n", ep.Backend)
	fmt.Fprintf(&report, "Trigger Reason:      %s\n", triggerReason)
	fmt.Fprintf(&report, "Last Recorded Error: %s\n", lastErr)
	fmt.Fprintf(&report, "Restart Attempt:     #%d of max %d\n", rec.Attempts, h.cfg.MaxRestartAttempts)
	report.WriteString("================================================================================\n")
	report.WriteString("--- [GPUStack Instance Container Logs (Tail 1000 Lines)] ---\n")
	report.WriteString(instanceLogs)
	report.WriteString("\n================================================================================\n")
	report.WriteString("[END OF CRASH AUDIT REPORT]\n")

	// Step 3: Save audit report to disk
	modelDir := ep.ModelName
	if modelDir == "" {
		modelDir = "default"
	}
	// Sanitize directory name for Windows
	modelDir = strings.ReplaceAll(modelDir, "/", "_")
	modelDir = strings.ReplaceAll(modelDir, "\\", "_")
	modelDir = strings.ReplaceAll(modelDir, ":", "_")

	targetDir := filepath.Join(h.cfg.CrashLogDir, modelDir)
	_ = os.MkdirAll(targetDir, 0755)

	timeFileStr := time.Now().Format("20060102_150405")
	instFileStr := ep.InstanceName
	if instFileStr == "" {
		instFileStr = fmt.Sprintf("instance_%d", ep.InstanceID)
	}
	instFileStr = strings.ReplaceAll(instFileStr, "/", "_")
	instFileStr = strings.ReplaceAll(instFileStr, "\\", "_")
	instFileStr = strings.ReplaceAll(instFileStr, ":", "_")

	filePath := filepath.Join(targetDir, fmt.Sprintf("%s_%s.log", instFileStr, timeFileStr))
	if writeErr := os.WriteFile(filePath, []byte(report.String()), 0644); writeErr != nil {
		log.Printf("[AutoHeal] Error writing crash audit report to %s: %v", filePath, writeErr)
	} else {
		log.Printf("[AutoHeal] 📝 Crash audit report safely archived to %s", filePath)
	}

	// Step 4: Call GPUStack RestartInstance API
	if restartErr := h.client.RestartInstance(ctx, ep.InstanceID); restartErr != nil {
		log.Printf("[AutoHeal] ❌ Failed to restart instance %d: %v", ep.InstanceID, restartErr)
		return filePath, restartErr
	}

	log.Printf("[AutoHeal] 🚀 Successfully sent restart command for instance %s (ID: %d). Instance is rebooting.",
		ep.InstanceName, ep.InstanceID)
	return filePath, nil
}
