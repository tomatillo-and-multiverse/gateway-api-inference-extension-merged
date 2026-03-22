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

package picker

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/observability/logging"
	fwkplugin "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrlatency "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/latency"
	attrprefix "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

const (
	AffinityWeightedPickerType = "affinity-weighted-picker"
)

// compile-time type validation
var _ framework.Picker = &AffinityWeightedPicker{}

// AffinityPickerConfig holds configuration for the affinity weighted picker.
type AffinityPickerConfig struct {
	// Tau is the global prefix cache score threshold (stage 1). Endpoints with
	// score >= Tau are considered sticky candidates. This is a strict gate
	// with TTFT load-gate protection.
	Tau float64 `json:"tau,omitempty"`

	// TauLocal is the local prefix cache score threshold (stage 2). After the
	// global gate narrows candidates, this further prefers endpoints with
	// score >= TauLocal. This is a softer gate (epsilon-only, no TTFT load gate).
	// Set to 0 to disable the local gate.
	TauLocal float64 `json:"tauLocal,omitempty"`

	// EpsilonExplore is the probability of ignoring the affinity gate and
	// selecting from all candidates (exploration). Range: [0, 1].
	EpsilonExplore float64 `json:"epsilonExplore,omitempty"`

	// MaxTTFTPenaltyMs is the maximum TTFT penalty (in ms) tolerated for
	// sticking to a high-affinity endpoint. If the best sticky endpoint's
	// predicted TTFT exceeds the best overall endpoint's TTFT by more than
	// this value, stickiness is broken. Set to 0 to disable the load gate.
	// Only applies to the global gate (Tau), not the local gate (TauLocal).
	MaxTTFTPenaltyMs float64 `json:"maxTTFTPenaltyMs,omitempty"`
}

var AffinityPickerDefaultConfig = AffinityPickerConfig{
	Tau:              0.99,
	TauLocal:         0.80,
	EpsilonExplore:   0.01,
	MaxTTFTPenaltyMs: 5000.0,
}

// AffinityWeightedPicker picks an endpoint using weighted random selection
// (A-Res algorithm) after applying a prefix cache affinity gate. It narrows
// candidates to sticky endpoints when appropriate, then selects from the
// narrowed set using the scores provided by the scorer.
//
// Logic:
//  1. If tau <= 0 (disabled), select from all scored endpoints.
//  2. Identify sticky endpoints (prefix cache score >= tau).
//  3. If no sticky endpoints, select from all.
//  4. With probability epsilon, select from all (explore).
//  5. If the best sticky endpoint's predicted TTFT exceeds the best
//     overall endpoint's TTFT by more than MaxTTFTPenaltyMs, select from
//     all (the queuing cost outweighs the cache benefit).
//  6. Otherwise, select from only the sticky endpoints.
type AffinityWeightedPicker struct {
	typedName fwkplugin.TypedName
	config    AffinityPickerConfig
}

// AffinityWeightedPickerFactory creates a new AffinityWeightedPicker plugin.
func AffinityWeightedPickerFactory(name string, rawParameters json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	config := AffinityPickerDefaultConfig
	if len(rawParameters) > 0 {
		if err := json.Unmarshal(rawParameters, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config for AffinityWeightedPicker: %w", err)
		}
	}
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid AffinityWeightedPicker config: %w", err)
	}
	return NewAffinityWeightedPicker(config).WithName(name), nil
}

func (c *AffinityPickerConfig) validate() error {
	if c.Tau < 0 || c.Tau > 1 {
		return fmt.Errorf("tau must be in [0, 1], got %f", c.Tau)
	}
	if c.TauLocal < 0 || c.TauLocal > 1 {
		return fmt.Errorf("tauLocal must be in [0, 1], got %f", c.TauLocal)
	}
	if c.EpsilonExplore < 0 || c.EpsilonExplore > 1 {
		return fmt.Errorf("epsilonExplore must be in [0, 1], got %f", c.EpsilonExplore)
	}
	if c.MaxTTFTPenaltyMs < 0 {
		return fmt.Errorf("maxTTFTPenaltyMs must be >= 0, got %f", c.MaxTTFTPenaltyMs)
	}
	return nil
}

// NewAffinityWeightedPicker creates a new AffinityWeightedPicker.
func NewAffinityWeightedPicker(config AffinityPickerConfig) *AffinityWeightedPicker {
	return &AffinityWeightedPicker{
		typedName: fwkplugin.TypedName{Type: AffinityWeightedPickerType, Name: AffinityWeightedPickerType},
		config:    config,
	}
}

func (p *AffinityWeightedPicker) WithName(name string) *AffinityWeightedPicker {
	p.typedName.Name = name
	return p
}

func (p *AffinityWeightedPicker) TypedName() fwkplugin.TypedName {
	return p.typedName
}

// Pick applies two stages of affinity gating, then selects one endpoint
// using weighted random selection (A-Res algorithm) based on scores.
//
// Stage 1 (global, Tau=0.99): strict gate with TTFT load-gate + epsilon.
// Stage 2 (local, TauLocal=0.80): softer gate with epsilon-only.
func (p *AffinityWeightedPicker) Pick(ctx context.Context, _ *framework.CycleState, scoredEndpoints []*framework.ScoredEndpoint) *framework.ProfileRunResult {
	// Stage 1: Global affinity gate (strict, with TTFT load gate).
	candidates := p.applyAffinityGate(ctx, scoredEndpoints)

	// Stage 2: Local affinity gate (softer, epsilon-only, no TTFT load gate).
	candidates = p.applyLocalAffinityGate(ctx, candidates)

	// A-Res weighted random selection from candidates.
	selected := p.weightedRandomSelect(candidates)

	return &framework.ProfileRunResult{TargetEndpoints: []framework.Endpoint{selected}}
}

// applyAffinityGate narrows scored endpoints to sticky candidates when appropriate.
func (p *AffinityWeightedPicker) applyAffinityGate(ctx context.Context, scoredEndpoints []*framework.ScoredEndpoint) []*framework.ScoredEndpoint {
	logger := log.FromContext(ctx)

	if p.config.Tau <= 0 {
		logger.V(logutil.DEBUG).Info("AffinityWeightedPicker: affinity gate disabled (tau <= 0)")
		return scoredEndpoints
	}

	// Identify sticky endpoints with high prefix cache scores.
	sticky := make([]*framework.ScoredEndpoint, 0, len(scoredEndpoints))
	for _, sep := range scoredEndpoints {
		score := prefixCacheScore(sep)
		if score >= p.config.Tau {
			sticky = append(sticky, sep)
		}
	}

	if len(sticky) == 0 {
		logger.V(logutil.DEBUG).Info("AffinityWeightedPicker: no sticky candidates found",
			"tau", p.config.Tau, "total", len(scoredEndpoints))
		return scoredEndpoints
	}

	// Epsilon-greedy exploration.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	if rng.Float64() < p.config.EpsilonExplore {
		logger.V(logutil.DEBUG).Info("AffinityWeightedPicker: exploring (epsilon)",
			"epsilon", p.config.EpsilonExplore, "sticky", len(sticky))
		return scoredEndpoints
	}

	// TTFT load gate: if the best sticky endpoint's predicted TTFT is too
	// much worse than the best overall, the cache benefit doesn't offset
	// the queuing cost — break stickiness.
	if p.config.MaxTTFTPenaltyMs > 0 {
		bestTTFTAll := bestScoredTTFT(scoredEndpoints)
		bestTTFTSticky := bestScoredTTFT(sticky)

		if bestTTFTAll < math.MaxFloat64 && bestTTFTSticky < math.MaxFloat64 {
			penalty := bestTTFTSticky - bestTTFTAll
			if penalty > p.config.MaxTTFTPenaltyMs {
				logger.V(logutil.DEBUG).Info("AffinityWeightedPicker: TTFT penalty too high, breaking stickiness",
					"bestStickyTTFT", bestTTFTSticky,
					"bestOverallTTFT", bestTTFTAll,
					"penaltyMs", penalty,
					"maxPenaltyMs", p.config.MaxTTFTPenaltyMs)
				return scoredEndpoints
			}
		}
	}

	logger.V(logutil.DEBUG).Info("AffinityWeightedPicker: sticking to sticky candidates",
		"tau", p.config.Tau, "sticky", len(sticky), "total", len(scoredEndpoints))
	return sticky
}

// applyLocalAffinityGate applies the second-stage (local) affinity gate.
// Same logic as the global gate (epsilon + TTFT load gate), but at a lower
// prefix cache threshold (default 0.80 vs 0.99).
func (p *AffinityWeightedPicker) applyLocalAffinityGate(ctx context.Context, scoredEndpoints []*framework.ScoredEndpoint) []*framework.ScoredEndpoint {
	logger := log.FromContext(ctx)

	if p.config.TauLocal <= 0 {
		return scoredEndpoints
	}

	sticky := make([]*framework.ScoredEndpoint, 0, len(scoredEndpoints))
	for _, sep := range scoredEndpoints {
		if prefixCacheScore(sep) >= p.config.TauLocal {
			sticky = append(sticky, sep)
		}
	}

	if len(sticky) == 0 {
		logger.V(logutil.DEBUG).Info("AffinityWeightedPicker: no local sticky candidates",
			"tauLocal", p.config.TauLocal, "total", len(scoredEndpoints))
		return scoredEndpoints
	}

	// Epsilon-greedy exploration.
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	if rng.Float64() < p.config.EpsilonExplore {
		logger.V(logutil.DEBUG).Info("AffinityWeightedPicker: local gate exploring (epsilon)",
			"tauLocal", p.config.TauLocal, "sticky", len(sticky))
		return scoredEndpoints
	}

	// TTFT load gate: same protection as global gate.
	if p.config.MaxTTFTPenaltyMs > 0 {
		bestTTFTAll := bestScoredTTFT(scoredEndpoints)
		bestTTFTSticky := bestScoredTTFT(sticky)

		if bestTTFTAll < math.MaxFloat64 && bestTTFTSticky < math.MaxFloat64 {
			penalty := bestTTFTSticky - bestTTFTAll
			if penalty > p.config.MaxTTFTPenaltyMs {
				logger.V(logutil.DEBUG).Info("AffinityWeightedPicker: local TTFT penalty too high, breaking stickiness",
					"tauLocal", p.config.TauLocal,
					"bestStickyTTFT", bestTTFTSticky,
					"bestOverallTTFT", bestTTFTAll,
					"penaltyMs", penalty,
					"maxPenaltyMs", p.config.MaxTTFTPenaltyMs)
				return scoredEndpoints
			}
		}
	}

	logger.V(logutil.DEBUG).Info("AffinityWeightedPicker: local gate sticking",
		"tauLocal", p.config.TauLocal, "sticky", len(sticky), "total", len(scoredEndpoints))
	return sticky
}

// weightedRandomSelect picks one endpoint using A-Res weighted random sampling.
func (p *AffinityWeightedPicker) weightedRandomSelect(candidates []*framework.ScoredEndpoint) framework.Endpoint {
	if len(candidates) == 1 {
		return candidates[0]
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Check if any candidate has a positive score.
	hasPositive := false
	for _, c := range candidates {
		if c.Score > 0 {
			hasPositive = true
			break
		}
	}

	// If all scores are zero, pick uniformly at random.
	if !hasPositive {
		return candidates[rng.Intn(len(candidates))]
	}

	// A-Res algorithm: key_i = U_i^(1/w_i)
	type keyed struct {
		ep  *framework.ScoredEndpoint
		key float64
	}
	keys := make([]keyed, len(candidates))
	for i, c := range candidates {
		if c.Score <= 0 {
			keys[i] = keyed{ep: c, key: 0}
			continue
		}
		u := rng.Float64()
		if u == 0 {
			u = 1e-10
		}
		keys[i] = keyed{ep: c, key: math.Pow(u, 1.0/c.Score)}
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].key > keys[j].key
	})

	return keys[0].ep
}

// prefixCacheScore returns the prefix cache score for an endpoint,
// reading from the PrefixCacheMatchInfo attribute.
func prefixCacheScore(ep framework.Endpoint) float64 {
	raw, ok := ep.Get(attrprefix.PrefixCacheMatchInfoKey)
	if !ok {
		return 0
	}
	info := raw.(*attrprefix.PrefixCacheMatchInfo)
	total := info.TotalBlocks()
	if total == 0 {
		return 0
	}
	score := float64(info.MatchBlocks()) / float64(total)
	if math.IsNaN(score) {
		return 0
	}
	return score
}

// bestScoredTTFT returns the lowest positive predicted TTFT across scored endpoints.
func bestScoredTTFT(endpoints []*framework.ScoredEndpoint) float64 {
	best := math.MaxFloat64
	for _, sep := range endpoints {
		raw, ok := sep.Get(attrlatency.LatencyPredictionInfoKey)
		if !ok {
			continue
		}
		info := raw.(*attrlatency.LatencyPredictionInfo)
		ttft := info.TTFT()
		if ttft > 0 && ttft < best {
			best = ttft
		}
	}
	return best
}
