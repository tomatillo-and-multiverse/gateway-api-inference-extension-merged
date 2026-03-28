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

// Package inputprofiletracker implements a PrepareDataPlugin that observes incoming request
// characteristics and tracks the effective input per request:
//
//	effectiveInput = inputTokens * (1 - prefixCacheScore)
//
// Input token count uses word count (len(strings.Fields(promptText))) to match the latency
// predictor training server's heuristic. Do NOT change this without retraining.
//
// The tracker selects the observation at the configured percentile of effective input and
// produces InputProfileInfo on endpoints, preserving both original (inputTokens,
// prefixCacheScore) values so the sidecar model gets the features it was trained on.
package inputprofiletracker

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/observability/logging"
	eppmetrics "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/metrics"
	fwkplugin "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/requestcontrol"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrinputprofile "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/inputprofile"
	attrprefix "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

const (
	// InputProfileTrackerType is the unique identifier for this plugin.
	InputProfileTrackerType = "input-profile-tracker"

	DefaultWindowDuration = "5m"
	DefaultMaxSamples     = 10000
	DefaultPercentile     = 90
)

// InputProfileProvider is the interface consumed by downstream plugins (e.g., the latency
// saturation detector) to get representative traffic characteristics for probing.
type InputProfileProvider interface {
	// ProbeProfile returns the representative (inputTokens, prefixCacheScore) pair
	// selected at the configured percentile of effective input.
	// Returns the fallback values if no observations exist.
	ProbeProfile(fallbackTokens int, fallbackCache float64) (inputTokens int, prefixCacheScore float64)
}

// Config holds the configuration for the input profile tracker.
type Config struct {
	// WindowDuration is how far back observations are kept (e.g., "5m", "300s").
	WindowDuration string `json:"windowDuration"`
	// MaxSamples caps the number of stored observations to bound memory.
	// When full, oldest entries are evicted.
	MaxSamples int `json:"maxSamples"`
	// Percentile (0-100) used for effective input token ranking (e.g., 90 = p90).
	Percentile int `json:"percentile"`

	// windowDuration is the parsed duration from WindowDuration.
	windowDuration time.Duration
}

type observation struct {
	timestamp            time.Time
	inputTokens          int
	prefixCacheScore     float64
	effectiveInputTokens int // inputTokens * (1 - prefixCacheScore)
}

var _ requestcontrol.PrepareDataPlugin = &Tracker{}

// Tracker observes effective input token counts from incoming requests.
type Tracker struct {
	config Config

	mu           sync.Mutex
	observations []observation
	writeIdx     int // ring buffer write position
}

// TrackerFactory creates a new input profile tracker plugin from config.
func TrackerFactory(_ string, params json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	config := Config{
		WindowDuration: DefaultWindowDuration,
		MaxSamples:     DefaultMaxSamples,
		Percentile:     DefaultPercentile,
	}
	if len(params) > 0 {
		if err := json.Unmarshal(params, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal input profile tracker config: %w", err)
		}
	}
	dur, err := time.ParseDuration(config.WindowDuration)
	if err != nil {
		return nil, fmt.Errorf("invalid windowDuration %q: %w", config.WindowDuration, err)
	}
	config.windowDuration = dur
	if config.Percentile < 0 || config.Percentile > 100 {
		return nil, fmt.Errorf("percentile must be 0-100, got %d", config.Percentile)
	}
	if config.MaxSamples <= 0 {
		config.MaxSamples = DefaultMaxSamples
	}
	return NewTracker(config), nil
}

// NewTracker creates a new input profile tracker.
func NewTracker(config Config) *Tracker {
	return &Tracker{
		config:       config,
		observations: make([]observation, 0, min(config.MaxSamples, 1024)),
	}
}

// TypedName returns the type and name tuple of this plugin instance.
func (t *Tracker) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{
		Type: InputProfileTrackerType,
		Name: InputProfileTrackerType,
	}
}

// PrepareRequestData computes the input token count (word count) from the request,
// reads prefix cache scores from endpoints, computes effective input, and produces
// InputProfileInfo on each endpoint.
func (t *Tracker) PrepareRequestData(ctx context.Context, request *framework.LLMRequest, endpoints []framework.Endpoint) error {
	logger := log.FromContext(ctx)

	inputTokens := countInputTokens(request)
	if inputTokens <= 0 {
		return nil
	}

	// Compute mean prefix cache score across endpoints for this request.
	prefixCacheScore := meanPrefixCacheScore(endpoints)

	// Effective input captures both request size and cache savings in one number.
	effectiveInput := int(math.Round(float64(inputTokens) * (1.0 - prefixCacheScore)))
	if effectiveInput < 1 {
		effectiveInput = 1
	}

	t.record(observation{
		timestamp:            time.Now(),
		inputTokens:          inputTokens,
		prefixCacheScore:     prefixCacheScore,
		effectiveInputTokens: effectiveInput,
	})

	// Produce the current profile snapshot as an attribute on every endpoint.
	probeTokens, probeCache := t.ProbeProfile(0, 0)
	if probeTokens > 0 {
		probeEffective := int(math.Round(float64(probeTokens) * (1.0 - probeCache)))
		profileInfo := attrinputprofile.NewInputProfileInfo(probeTokens, probeCache, probeEffective)
		for _, ep := range endpoints {
			ep.Put(attrinputprofile.InputProfileInfoKey, profileInfo)
		}
		eppmetrics.RecordInputProfileTrackerStats(probeTokens, probeCache, probeEffective, t.observationCount())
	}

	logger.V(logutil.TRACE).Info("Input profile observation",
		"inputTokens", inputTokens,
		"prefixCacheScore", prefixCacheScore,
		"effectiveInput", effectiveInput)

	return nil
}

// Produces declares that this plugin produces InputProfileInfo on endpoints.
func (t *Tracker) Produces() map[string]any {
	return map[string]any{
		attrinputprofile.InputProfileInfoKey: attrinputprofile.InputProfileInfo{},
	}
}

// Consumes declares that this plugin reads prefix cache match info from endpoints (if available).
func (t *Tracker) Consumes() map[string]any {
	return map[string]any{attrprefix.PrefixCacheMatchInfoKey: attrprefix.PrefixCacheMatchInfo{}}
}

// ProbeProfile returns the (inputTokens, prefixCacheScore) pair from the observation at
// the configured percentile of effective input.
func (t *Tracker) ProbeProfile(fallbackTokens int, fallbackCache float64) (int, float64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	valid := t.validObservations()
	if len(valid) == 0 {
		return fallbackTokens, fallbackCache
	}

	// Sort by effective input tokens.
	sort.Slice(valid, func(i, j int) bool {
		return valid[i].effectiveInputTokens < valid[j].effectiveInputTokens
	})

	idx := percentileIndex(len(valid), t.config.Percentile)
	return valid[idx].inputTokens, valid[idx].prefixCacheScore
}

// observationCount returns the number of valid (non-expired) observations.
func (t *Tracker) observationCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.validObservations())
}

// record appends an observation to the ring buffer.
func (t *Tracker) record(obs observation) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.observations) < t.config.MaxSamples {
		t.observations = append(t.observations, obs)
	} else {
		t.observations[t.writeIdx] = obs
		t.writeIdx = (t.writeIdx + 1) % t.config.MaxSamples
	}
}

// validObservations returns observations within the time window. Must be called with mu held.
func (t *Tracker) validObservations() []observation {
	cutoff := time.Now().Add(-t.config.windowDuration)
	valid := make([]observation, 0, len(t.observations))
	for _, o := range t.observations {
		if o.timestamp.After(cutoff) {
			valid = append(valid, o)
		}
	}
	return valid
}

// countInputTokens computes the input token count using word count.
// This MUST match the training server's heuristic: len(strings.Fields(promptText)).
// Do NOT change to actual token IDs or byte-based estimates without retraining.
func countInputTokens(request *framework.LLMRequest) int {
	if request == nil || request.Body == nil {
		return 0
	}
	return len(strings.Fields(request.Body.PromptText()))
}

// meanPrefixCacheScore computes the mean prefix cache score across all endpoints.
func meanPrefixCacheScore(endpoints []framework.Endpoint) float64 {
	var sum float64
	var count int
	for _, ep := range endpoints {
		raw, ok := ep.Get(attrprefix.PrefixCacheMatchInfoKey)
		if !ok {
			continue
		}
		info, ok := raw.(*attrprefix.PrefixCacheMatchInfo)
		if !ok || info == nil || info.TotalBlocks() == 0 {
			continue
		}
		score := float64(info.MatchBlocks()) / float64(info.TotalBlocks())
		if !math.IsNaN(score) {
			sum += score
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

// percentileIndex returns the index for the given percentile in a sorted slice of length n.
func percentileIndex(n, percentile int) int {
	if n == 0 {
		return 0
	}
	idx := (percentile * n) / 100
	if idx >= n {
		idx = n - 1
	}
	return idx
}
