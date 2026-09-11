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

// ShouldRestart performs a local fast check whether an unhealthy worker meets restart criteria.
func (h *AutoHealer) ShouldRestart(openSince time.Time, lastErr string) (bool, string) {
	if !h.cfg.Enabled {
		return false, ""
	}

	// 1. Check fatal keywords in local probe error
	if lastErr != "" {
		for _, kw := range h.cfg.FatalKeywords {
			if strings.Contains(strings.ToLower(lastErr), strings.ToLower(kw)) {
				return true, fmt.Sprintf("Matched fatal error keyword %q in probe error: %s", kw, lastErr)
			}
		}
	}

	// 2. Check unhealthy timeout (persistent continuous OPEN state)
	if !openSince.IsZero() && time.Since(openSince) >= h.cfg.UnhealthyTimeout {
		return true, fmt.Sprintf("Unhealthy timeout exceeded: in OPEN state for %v (threshold: %v)",
			time.Since(openSince).Round(time.Second), h.cfg.UnhealthyTimeout)
	}

	return false, ""
}

// EvaluateAndTrigger inspects live GPUStack instance state, protects boot/download states,
// validates fatal keywords from both StateMessage and probe errors, archives logs, and triggers restart.
func (h *AutoHealer) EvaluateAndTrigger(ctx context.Context, ep WorkerEndpoint, openSince time.Time, lastErr string) (string, error) {
	if !h.cfg.Enabled || h.client == nil || ep.InstanceID <= 0 {
		return "", nil
	}

	// 1. Fetch live instance status from GPUStack to inspect authoritative State and StateMessage
	liveInst, err := h.client.GetInstance(ctx, ep.InstanceID)
	if err != nil {
		log.Printf("[AutoHeal] Warning: failed to query live GPUStack instance %d: %v. Falling back to probe status.", ep.InstanceID, err)
	}

	// 2. Booting Guard: if instance is currently downloading/initializing/starting, DO NOT restart!
	if liveInst != nil {
		st := strings.ToLower(liveInst.State)
		switch st {
		case "starting", "downloading", "initializing", "pending", "scheduled", "analyzing":
			log.Printf("[AutoHeal] ⏳ Booting Guard: Instance %s (ID: %d) is in state %q. Skipping restart to allow cold-boot.",
				ep.InstanceName, ep.InstanceID, liveInst.State)
			return "", nil
		}
	}

	// 3. Evaluate restart criteria
	var shouldRestart bool
	var triggerReason string

	if liveInst != nil {
		// A. Check GPUStack authoritative StateMessage for fatal keywords (e.g. CUDA OOM, NCCL)
		if liveInst.StateMessage != "" {
			for _, kw := range h.cfg.FatalKeywords {
				if strings.Contains(strings.ToLower(liveInst.StateMessage), strings.ToLower(kw)) {
					shouldRestart = true
					triggerReason = fmt.Sprintf("GPUStack StateMessage matched fatal keyword %q: %s", kw, liveInst.StateMessage)
					break
				}
			}
		}

		// B. If GPUStack reports state "error" or "unreachable", it is an authoritative failure
		if !shouldRestart && (strings.EqualFold(liveInst.State, "error") || strings.EqualFold(liveInst.State, "unreachable")) {
			shouldRestart = true
			triggerReason = fmt.Sprintf("GPUStack authoritative state is %q (Message: %s)", liveInst.State, liveInst.StateMessage)
		}
	}

	// C. Fallback: check local probe error and continuous open duration
	if !shouldRestart {
		shouldRestart, triggerReason = h.ShouldRestart(openSince, lastErr)
	}

	if !shouldRestart {
		return "", nil
	}

	// 4. Trigger incident mitigation (log preservation + restart)
	stateMsg := ""
	if liveInst != nil {
		stateMsg = liveInst.StateMessage
	}
	return h.triggerMitigation(ctx, ep, triggerReason, lastErr, stateMsg)
}

// TriggerIncidentRestart directly executes crash log capture and instance restart with rate limiting.
func (h *AutoHealer) TriggerIncidentRestart(ctx context.Context, ep WorkerEndpoint, triggerReason string, lastErr string) (string, error) {
	if !h.cfg.Enabled || h.client == nil || ep.InstanceID <= 0 {
		return "", nil
	}
	return h.triggerMitigation(ctx, ep, triggerReason, lastErr, "")
}

func (h *AutoHealer) triggerMitigation(ctx context.Context, ep WorkerEndpoint, triggerReason string, lastErr string, stateMsg string) (string, error) {
	h.mu.Lock()
	rec, exists := h.records[ep.InstanceID]
	if !exists {
		rec = &RestartRecord{}
		h.records[ep.InstanceID] = rec
	}

	// Guard against infinite restart loops
	if !rec.LastRestart.IsZero() && time.Since(rec.LastRestart) < h.cfg.RestartCooldown {
		rem := (h.cfg.RestartCooldown - time.Since(rec.LastRestart)).Round(time.Second)
		h.mu.Unlock()
		log.Printf("[AutoHeal] ⚠️ Instance %s (ID: %d) restart skipped: within cooldown (%v remaining)",
			ep.InstanceName, ep.InstanceID, rem)
		return "", nil
	}

	if rec.Attempts >= h.cfg.MaxRestartAttempts {
		maxAtt := h.cfg.MaxRestartAttempts
		h.mu.Unlock()
		log.Printf("[AutoHeal] 🚨 CRITICAL: Instance %s (ID: %d) reached maximum restart attempts (%d)! Halting auto-restart to prevent flapping.",
			ep.InstanceName, ep.InstanceID, maxAtt)
		return "", fmt.Errorf("instance %d exceeded maximum restart attempts (%d)", ep.InstanceID, maxAtt)
	}

	rec.Attempts++
	rec.LastRestart = time.Now()
	attemptCount := rec.Attempts
	maxAttempts := h.cfg.MaxRestartAttempts
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
	if stateMsg != "" {
		fmt.Fprintf(&report, "GPUStack StateMsg:   %s\n", stateMsg)
	}
	fmt.Fprintf(&report, "Restart Attempt:     #%d of max %d\n", attemptCount, maxAttempts)
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

	// Step 4: Call GPUStack RestartInstance API (deletes instance so GPUStack scheduler recreates it)
	if restartErr := h.client.RestartInstance(ctx, ep.InstanceID); restartErr != nil {
		log.Printf("[AutoHeal] ❌ Failed to restart instance %d: %v", ep.InstanceID, restartErr)
		return filePath, restartErr
	}

	log.Printf("[AutoHeal] 🚀 Successfully sent restart command for instance %s (ID: %d). Instance is rebooting.",
		ep.InstanceName, ep.InstanceID)
	return filePath, nil
}

// ResetAttempts resets the restart attempts counter when an instance recovers and operates healthily.
func (h *AutoHealer) ResetAttempts(instanceID int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rec, ok := h.records[instanceID]; ok {
		if rec.Attempts > 0 {
			log.Printf("[AutoHeal] 🟢 Instance %d recovered and healthy. Resetting restart attempts from %d to 0.",
				instanceID, rec.Attempts)
			rec.Attempts = 0
		}
	}
}

// GetRecord returns a snapshot copy of the restart record for the given instance ID.
func (h *AutoHealer) GetRecord(instanceID int) (RestartRecord, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rec, ok := h.records[instanceID]; ok {
		return *rec, true
	}
	return RestartRecord{}, false
}
