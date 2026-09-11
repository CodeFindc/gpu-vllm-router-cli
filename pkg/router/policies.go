package router

import (
	"fmt"
	"strings"
)

// Policy represents a load balancing policy supported by vllm-router.
type Policy string

const (
	PolicyCacheAware     Policy = "cache_aware"
	PolicyConsistentHash Policy = "consistent_hash"
	PolicyRoundRobin     Policy = "round_robin"
	PolicyPowerOfTwo     Policy = "power_of_two"
	PolicyRandom         Policy = "random"
	PolicyRendezvousHash Policy = "rendezvous_hash"
)

// PolicyInfo holds description and metadata for a policy.
type PolicyInfo struct {
	Name            Policy `json:"name"`
	Description     string `json:"description"`
	BestFor         string `json:"best_for"`
	SessionAffinity bool   `json:"session_affinity"`
	LoadAware       bool   `json:"load_aware"`
}

// SupportedPolicies returns the list of all supported policies and their descriptions.
func SupportedPolicies() []PolicyInfo {
	return []PolicyInfo{
		{
			Name:            PolicyCacheAware,
			Description:     "Prefix-caching optimization aware routing",
			BestFor:         "Workloads with common prefixes/prompts, KV cache reuse",
			SessionAffinity: true,
			LoadAware:       true,
		},
		{
			Name:            PolicyConsistentHash,
			Description:     "Consistent hash ring based on session/user ID",
			BestFor:         "Multi-turn conversations, stateful sessions, KV cache locality",
			SessionAffinity: true,
			LoadAware:       false,
		},
		{
			Name:            PolicyRoundRobin,
			Description:     "Evenly rotates requests among all healthy workers",
			BestFor:         "General-purpose load balancing across identical workers",
			SessionAffinity: false,
			LoadAware:       false,
		},
		{
			Name:            PolicyPowerOfTwo,
			Description:     "P2C - picks least busy among two random candidates",
			BestFor:         "Load-sensitive workloads with varying request lengths",
			SessionAffinity: false,
			LoadAware:       true,
		},
		{
			Name:            PolicyRandom,
			Description:     "Uniform random distribution across workers",
			BestFor:         "Simple deployments, stateless queries",
			SessionAffinity: false,
			LoadAware:       false,
		},
		{
			Name:            PolicyRendezvousHash,
			Description:     "Highest Random Weight (HRW) hashing",
			BestFor:         "Distributed cache affinity with minimal migration on node churn",
			SessionAffinity: true,
			LoadAware:       false,
		},
	}
}

// ParsePolicy parses and validates a policy string.
func ParsePolicy(s string) (Policy, error) {
	norm := strings.ToLower(strings.TrimSpace(s))
	// Replace dashes with underscores for user convenience (e.g. cache-aware -> cache_aware)
	norm = strings.ReplaceAll(norm, "-", "_")

	switch Policy(norm) {
	case PolicyCacheAware, "cache":
		return PolicyCacheAware, nil
	case PolicyConsistentHash, "hash", "chash":
		return PolicyConsistentHash, nil
	case PolicyRoundRobin, "rr":
		return PolicyRoundRobin, nil
	case PolicyPowerOfTwo, "p2c", "power_two":
		return PolicyPowerOfTwo, nil
	case PolicyRandom, "rand":
		return PolicyRandom, nil
	case PolicyRendezvousHash, "hrw", "rendezvous":
		return PolicyRendezvousHash, nil
	default:
		return "", fmt.Errorf("unknown load balancing policy: %q (supported: cache_aware, consistent_hash, round_robin, power_of_two, random, rendezvous_hash)", s)
	}
}
