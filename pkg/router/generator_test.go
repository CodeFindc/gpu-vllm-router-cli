package router

import (
	"strings"
	"testing"
)

func TestBuildArgs(t *testing.T) {
	cfg := Config{
		Host:       "127.0.0.1",
		Port:       8080,
		Policy:     PolicyConsistentHash,
		WorkerURLs: []string{"http://192.168.1.10:40039", "http://192.168.1.11:40039"},
	}

	args := BuildArgs(cfg)
	cmdStr := strings.Join(args, " ")

	if !strings.Contains(cmdStr, "--host 127.0.0.1") {
		t.Errorf("expected --host 127.0.0.1, got %s", cmdStr)
	}
	if !strings.Contains(cmdStr, "--port 8080") {
		t.Errorf("expected --port 8080, got %s", cmdStr)
	}
	if !strings.Contains(cmdStr, "--policy consistent_hash") {
		t.Errorf("expected --policy consistent_hash, got %s", cmdStr)
	}
	if !strings.Contains(cmdStr, "--worker-urls http://192.168.1.10:40039 http://192.168.1.11:40039") {
		t.Errorf("expected worker URLs in command, got %s", cmdStr)
	}

	bashCmd := GenerateBashCommand(cfg)
	if !strings.HasPrefix(bashCmd, "vllm-router") {
		t.Errorf("expected bash command to start with vllm-router, got %s", bashCmd)
	}

	psCmd := GeneratePowerShellCommand(cfg)
	if !strings.Contains(psCmd, "`") {
		t.Errorf("expected powershell line continuations, got %s", psCmd)
	}
}

func TestParsePolicy(t *testing.T) {
	tests := []struct {
		input    string
		expected Policy
		hasError bool
	}{
		{"round_robin", PolicyRoundRobin, false},
		{"RR", PolicyRoundRobin, false},
		{"consistent_hash", PolicyConsistentHash, false},
		{"consistent-hash", PolicyConsistentHash, false},
		{"cache_aware", PolicyCacheAware, false},
		{"power_of_two", PolicyPowerOfTwo, false},
		{"p2c", PolicyPowerOfTwo, false},
		{"random", PolicyRandom, false},
		{"rendezvous_hash", PolicyRendezvousHash, false},
		{"invalid_policy", "", true},
	}

	for _, tc := range tests {
		p, err := ParsePolicy(tc.input)
		if tc.hasError && err == nil {
			t.Errorf("expected error for %q, got nil", tc.input)
		}
		if !tc.hasError && (err != nil || p != tc.expected) {
			t.Errorf("for input %q: expected %q, got %q (err: %v)", tc.input, tc.expected, p, err)
		}
	}
}

func TestBuildArgsWithCircuitBreaker(t *testing.T) {
	cfg := Config{
		Host:                    "0.0.0.0",
		Port:                    8000,
		Policy:                  PolicyConsistentHash,
		WorkerURLs:              []string{"http://10.0.0.1:8000", "http://10.0.0.2:8000"},
		CbFailureThreshold:      5,
		CbSuccessThreshold:      2,
		CbTimeoutDurationSecs:   30,
		CbWindowDurationSecs:    60,
		RetryMaxRetries:         3,
		RetryInitialBackoffMs:   50,
		HealthCheckIntervalSecs: 10,
		HealthCheckTimeoutSecs:  5,
		HealthCheckEndpoint:     "/health",
	}

	args := BuildArgs(cfg)
	cmdStr := strings.Join(args, " ")

	expectedFlags := []string{
		"--cb-failure-threshold 5",
		"--cb-success-threshold 2",
		"--cb-timeout-duration-secs 30",
		"--cb-window-duration-secs 60",
		"--retry-max-retries 3",
		"--retry-initial-backoff-ms 50",
		"--health-check-interval-secs 10",
		"--health-check-timeout-secs 5",
		"--health-check-endpoint /health",
	}

	for _, flag := range expectedFlags {
		if !strings.Contains(cmdStr, flag) {
			t.Errorf("expected command string to contain %q, but got: %s", flag, cmdStr)
		}
	}

	// Test disable flags
	cfgDisabled := Config{
		Host:                  "0.0.0.0",
		Port:                  8000,
		WorkerURLs:            []string{"http://10.0.0.1:8000"},
		DisableCircuitBreaker: true,
		DisableRetries:        true,
	}
	cmdDisabledStr := strings.Join(BuildArgs(cfgDisabled), " ")
	if !strings.Contains(cmdDisabledStr, "--disable-circuit-breaker") {
		t.Errorf("expected --disable-circuit-breaker in %s", cmdDisabledStr)
	}
	if !strings.Contains(cmdDisabledStr, "--disable-retries") {
		t.Errorf("expected --disable-retries in %s", cmdDisabledStr)
	}
}

func TestBuildArgsWithThresholdsAndExtraArgs(t *testing.T) {
	cfg := Config{
		Host:                "0.0.0.0",
		Port:                8000,
		Policy:              PolicyCacheAware,
		WorkerURLs:          []string{"http://10.0.0.1:8000", "http://10.0.0.2:8000"},
		BalanceAbsThreshold: 4,
		BalanceRelThreshold: 1.1,
		CacheThreshold:      0.6,
		ExtraArgs:           []string{"--request-timeout", "60"},
	}

	args := BuildArgs(cfg)
	cmdStr := strings.Join(args, " ")

	expectedFlags := []string{
		"--policy cache_aware",
		"--balance-abs-threshold 4",
		"--balance-rel-threshold 1.1",
		"--cache-threshold 0.6",
		"--request-timeout 60",
	}

	for _, flag := range expectedFlags {
		if !strings.Contains(cmdStr, flag) {
			t.Errorf("expected command string to contain %q, but got: %s", flag, cmdStr)
		}
	}
}

func TestBuildArgsWithPDAndAdvancedTuning(t *testing.T) {
	cfg := Config{
		Host:                       "0.0.0.0",
		Port:                       8001,
		Policy:                     PolicyConsistentHash,
		Backend:                    "sglang",
		LogLevel:                   "debug",
		LogDir:                     "/var/log/vllm",
		VllmPDDisagg:               true,
		VllmDiscoveryAddress:       "0.0.0.0:30001",
		PrefillPolicy:              "cache_aware",
		DecodePolicy:               "power_of_two",
		PrefillURLs:                []string{"http://10.0.1.1:8000"},
		DecodeURLs:                 []string{"http://10.0.1.2:8000"},
		WorkerStartupTimeoutSecs:   300,
		WorkerStartupCheckInterval: 15,
		EvictionInterval:           240,
		MaxTreeSize:                1048576,
		MaxPayloadSize:             104857600,
		APIKey:                     "sk-secret",
		APIKeyValidationURLs:       []string{"http://auth.local/val"},
	}

	args := BuildArgs(cfg)
	cmdStr := strings.Join(args, " ")

	expectedFlags := []string{
		"--backend sglang",
		"--log-level debug",
		"--log-dir /var/log/vllm",
		"--vllm-pd-disaggregation",
		"--vllm-discovery-address 0.0.0.0:30001",
		"--prefill-policy cache_aware",
		"--decode-policy power_of_two",
		"--prefill http://10.0.1.1:8000",
		"--decode http://10.0.1.2:8000",
		"--worker-startup-timeout-secs 300",
		"--worker-startup-check-interval 15",
		"--eviction-interval 240",
		"--max-tree-size 1048576",
		"--max-payload-size 104857600",
		"--api-key sk-secret",
		"--api-key-validation-urls http://auth.local/val",
	}

	for _, flag := range expectedFlags {
		if !strings.Contains(cmdStr, flag) {
			t.Errorf("expected command to contain %q, but got:\n%s", flag, cmdStr)
		}
	}

	bashCmd := GenerateBashCommand(cfg)
	if !strings.Contains(bashCmd, "--vllm-pd-disaggregation") {
		t.Errorf("expected bash command to contain --vllm-pd-disaggregation, got %s", bashCmd)
	}

	dockerCmd := GenerateDockerCommand(cfg, "vllm/vllm-router:latest")
	if !strings.Contains(dockerCmd, "--backend sglang") {
		t.Errorf("expected docker command to contain --backend sglang, got %s", dockerCmd)
	}
	if !strings.Contains(dockerCmd, "-v /var/log/vllm:/var/log/vllm") {
		t.Errorf("expected docker command to contain custom LogDir volume mount, got %s", dockerCmd)
	}

	cfgDefaultLog := Config{Port: 8080}
	dockerCmdDefault := GenerateDockerCommand(cfgDefaultLog, "vllm/vllm-router:latest")
	if !strings.Contains(dockerCmdDefault, "-v $(pwd)/logs:/app/logs") {
		t.Errorf("expected docker command with empty LogDir to contain default logs volume mount, got %s", dockerCmdDefault)
	}
}

