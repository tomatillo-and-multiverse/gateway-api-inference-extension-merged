/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package latencydetector

import (
	"fmt"
	"time"
)

// Default configuration values.
const (
	// DefaultE2ESLOMs is the default E2E latency SLO in milliseconds.
	DefaultE2ESLOMs = 200.0
	// DefaultProbeInputTokenLength is the representative input token count used when
	// probing endpoints. It should approximate the p90 of real traffic.
	DefaultProbeInputTokenLength = 512
	// DefaultProbePrefixCacheScore is the assumed prefix cache hit score for probes.
	// 0 represents the worst case (no cache reuse).
	DefaultProbePrefixCacheScore = 0.0
	// DefaultProbeInterval is how often the background goroutine probes endpoints.
	DefaultProbeInterval = "10s"
	// DefaultHeadroom is the burst allowance fraction for the Filter (0%).
	DefaultHeadroom = 0.0
)

// Config holds the configuration for the latency-based saturation detector.
//
// Exactly one of E2ESLOMs or TTFTSLOMs must be set (> 0). This determines the mode:
//
//   - E2ESLOMs > 0 (non-streaming): The predictor's TTFT output represents E2E request
//     latency. TPOT is ignored entirely. This is the default.
//
//   - TTFTSLOMs > 0 (streaming): The predictor outputs real TTFT and TPOT.
//     TPOTSLOMs is optional; when > 0, both are checked.
type Config struct {
	// E2ESLOMs is the end-to-end latency SLO in milliseconds.
	// Set this for non-streaming workloads where the predictor's TTFT output
	// represents the full request completion time. TPOT is ignored.
	// Mutually exclusive with TTFTSLOMs.
	E2ESLOMs float64 `json:"e2eSLOMs"`

	// TTFTSLOMs is the time-to-first-token SLO in milliseconds.
	// Set this for streaming workloads. Mutually exclusive with E2ESLOMs.
	TTFTSLOMs float64 `json:"ttftSLOMs"`

	// TPOTSLOMs is the time-per-output-token SLO in milliseconds.
	// Only used when TTFTSLOMs is set (streaming mode). When 0, only TTFT is checked.
	TPOTSLOMs float64 `json:"tpotSLOMs"`

	// ProbeInputTokenLength is the representative input token count for probing.
	// The background probe sends a synthetic request with this many input tokens
	// to each endpoint. Should approximate the p90 of real traffic.
	ProbeInputTokenLength int `json:"probeInputTokenLength"`

	// ProbePrefixCacheScore is the assumed prefix cache hit ratio for probes (0.0 to 1.0).
	// 0 = worst case (no cache), 1 = best case (full cache hit).
	ProbePrefixCacheScore float64 `json:"probePrefixCacheScore"`

	// ProbeInterval controls how often the background goroutine probes all endpoints (e.g., "10s").
	ProbeInterval string `json:"probeInterval"`

	// probeInterval is the parsed duration from ProbeInterval.
	probeInterval time.Duration

	// Headroom defines the allowed burst capacity above SLO for the Filter,
	// expressed as a fraction in [0.0, 1.0].
	// Example: E2ESLOMs=200, Headroom=0.2 → Filter allows predicted latency up to 240ms.
	Headroom float64 `json:"headroom"`
}

// isStreaming returns true when the config targets a streaming workload (TTFTSLOMs set).
func (c *Config) isStreaming() bool {
	return c.TTFTSLOMs > 0
}

// activeTTFTSLO returns the SLO used for the predictor's TTFT output.
// In non-streaming mode this is E2ESLOMs; in streaming mode it is TTFTSLOMs.
func (c *Config) activeTTFTSLO() float64 {
	if c.isStreaming() {
		return c.TTFTSLOMs
	}
	return c.E2ESLOMs
}

// validate checks that the config is internally consistent and parses durations.
func (c *Config) validate() error {
	if c.E2ESLOMs > 0 && c.TTFTSLOMs > 0 {
		return fmt.Errorf("e2eSLOMs and ttftSLOMs are mutually exclusive; set one or the other")
	}
	if c.E2ESLOMs <= 0 && c.TTFTSLOMs <= 0 {
		return fmt.Errorf("one of e2eSLOMs (non-streaming) or ttftSLOMs (streaming) must be > 0")
	}
	if c.TPOTSLOMs > 0 && !c.isStreaming() {
		return fmt.Errorf("tpotSLOMs requires ttftSLOMs (streaming mode); set ttftSLOMs instead of e2eSLOMs")
	}
	if c.ProbeInterval != "" {
		dur, err := time.ParseDuration(c.ProbeInterval)
		if err != nil {
			return fmt.Errorf("invalid probeInterval %q: %w", c.ProbeInterval, err)
		}
		c.probeInterval = dur
	}
	return nil
}
