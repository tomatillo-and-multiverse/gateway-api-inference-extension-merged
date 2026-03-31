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

// Package latency_test contains pipeline equivalence tests that verify the
// decomposed filter+scorer pipeline produces identical results to the
// reference monolith logic for all meaningful input scenarios.
//
// Test structure:
//   - runReference: implements the full monolith logic as plain Go — this is the ground truth.
//   - runDecomposed: chains the actual plugin instances (filters + scorer).
//   - Both are run on the same endpoint set with epsilons pinned (0 or 1) for determinism.
//   - Outputs are compared by exact score map equality, not just ranking.
package latency_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrlatency "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/latency"
	attrprefix "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	prefixaffinity "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/scheduling/filter/prefixcacheaffinity"
	sloheadroomtier "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/scheduling/filter/sloheadroomtier"
	latencyscorer "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/scheduling/scorer/latency"
)

// ── Endpoint construction ─────────────────────────────────────────────────────

type endpointSpec struct {
	name string

	// Latency prediction — set noPrediction=true to omit LatencyPredictionInfo.
	noPrediction    bool
	ttftHeadroom    float64
	tpotHeadroom    float64
	ttft            float64 // predicted TTFT (ms), used by TTFT load gate
	dispatchedCount int

	// Prefix cache
	prefixMatchBlocks int
	prefixTotalBlocks int // 0 = no PrefixCacheMatchInfo

	// Composite fallback metrics
	kvUtilPercent float64 // [0,1]
	queueSize     int
}

func makeEndpoint(t *testing.T, s endpointSpec) framework.Endpoint {
	t.Helper()
	meta := &fwkdl.EndpointMetadata{
		NamespacedName: types.NamespacedName{Name: s.name, Namespace: "test"},
	}
	metrics := &fwkdl.Metrics{
		KVCacheUsagePercent: s.kvUtilPercent,
		WaitingQueueSize:    s.queueSize,
	}
	ep := framework.NewEndpoint(meta, metrics, fwkdl.NewAttributes())

	if !s.noPrediction {
		ep.Put(attrlatency.LatencyPredictionInfoKey,
			attrlatency.NewLatencyPredictionInfoWithDispatch(
				true, true,
				s.ttftHeadroom, s.tpotHeadroom,
				s.ttft, 0,
				s.dispatchedCount,
			))
	}

	if s.prefixTotalBlocks > 0 {
		ep.Put(attrprefix.PrefixCacheMatchInfoKey,
			attrprefix.NewPrefixCacheMatchInfo(s.prefixMatchBlocks, s.prefixTotalBlocks, 16))
	}

	return ep
}

// ── Pipeline config ───────────────────────────────────────────────────────────

// pipelineConfig fully describes both pipelines. Epsilons must be 0 or 1 for
// deterministic tests.
type pipelineConfig struct {
	// Affinity filter (used twice, same config both instances)
	globalTau        float64
	localTau         float64
	maxTTFTPenaltyMs float64
	affinityEpsilon  float64 // 0 = always apply gate; 1 = always skip gate

	// Tier filter
	tierEpsilon float64 // 0 = always positive tier; 1 = always negative tier

	// Scorer
	ttftWeight float64
	tpotWeight float64
	strategy   latencyscorer.HeadroomSelectionStrategy

	// Composite fallback
	compositeKVWeight     float64
	compositeQueueWeight  float64
	compositePrefixWeight float64
}

func defaultConfig() pipelineConfig {
	return pipelineConfig{
		globalTau:             0.99,
		localTau:              0.80,
		maxTTFTPenaltyMs:      5000,
		affinityEpsilon:       0,
		tierEpsilon:           0,
		ttftWeight:            0.8,
		tpotWeight:            0.2,
		strategy:              latencyscorer.StrategyLeast,
		compositeKVWeight:     1,
		compositeQueueWeight:  1,
		compositePrefixWeight: 1,
	}
}

// ── Reference monolith implementation ────────────────────────────────────────
//
// This is the ground truth: a plain-Go reimplementation of the full monolith
// pipeline. No plugin instances — just the logic, readable and auditable.

func referenceAffinityGate(
	endpoints []framework.Endpoint,
	tau, epsilon, maxTTFTPenaltyMs float64,
) []framework.Endpoint {
	if len(endpoints) <= 1 || tau <= 0 {
		return endpoints
	}
	// epsilon=1 means always skip (for test determinism)
	if epsilon >= 1.0 {
		return endpoints
	}
	// epsilon=0 means never skip — proceed with gate.

	var sticky, nonSticky []framework.Endpoint
	for _, ep := range endpoints {
		if referencePrefixScore(ep) >= tau {
			sticky = append(sticky, ep)
		} else {
			nonSticky = append(nonSticky, ep)
		}
	}

	if len(sticky) == 0 {
		return endpoints
	}

	// TTFT load gate: break stickiness if cached endpoints are too slow.
	if maxTTFTPenaltyMs > 0 && len(nonSticky) > 0 {
		bestSticky := referenceBestTTFT(sticky)
		bestNonSticky := referenceBestTTFT(nonSticky)
		if bestSticky-bestNonSticky > maxTTFTPenaltyMs {
			return endpoints
		}
	}

	return sticky
}

func referenceTierSplit(endpoints []framework.Endpoint, epsilon float64) []framework.Endpoint {
	if len(endpoints) <= 1 {
		return endpoints
	}

	var positive, negative, noPred []framework.Endpoint
	for _, ep := range endpoints {
		raw, ok := ep.Get(attrlatency.LatencyPredictionInfoKey)
		if !ok {
			noPred = append(noPred, ep)
			continue
		}
		info := raw.(*attrlatency.LatencyPredictionInfo)
		if info.TTFTHeadroom() >= 0 && info.TPOTHeadroom() >= 0 {
			positive = append(positive, ep)
		} else {
			negative = append(negative, ep)
		}
	}

	// No predictions at all → keep all.
	if len(positive) == 0 && len(negative) == 0 {
		return endpoints
	}

	// noPred endpoints fall into the negative tier.
	negative = append(negative, noPred...)

	// epsilon=1 → always explore negative tier.
	if epsilon >= 1.0 && len(negative) > 0 {
		return negative
	}

	if len(positive) > 0 {
		return positive
	}
	return negative
}

func referencePrefixScore(ep framework.Endpoint) float64 {
	raw, ok := ep.Get(attrprefix.PrefixCacheMatchInfoKey)
	if !ok {
		return 0
	}
	info := raw.(*attrprefix.PrefixCacheMatchInfo)
	if info.TotalBlocks() == 0 {
		return 0
	}
	score := float64(info.MatchBlocks()) / float64(info.TotalBlocks())
	if math.IsNaN(score) {
		return 0
	}
	return score
}

func referenceBestTTFT(endpoints []framework.Endpoint) float64 {
	best := math.MaxFloat64
	for _, ep := range endpoints {
		raw, ok := ep.Get(attrlatency.LatencyPredictionInfoKey)
		if !ok {
			continue
		}
		info := raw.(*attrlatency.LatencyPredictionInfo)
		if info.TTFT() < best {
			best = info.TTFT()
		}
	}
	return best
}

// runReference executes the full monolith pipeline as a reference implementation.
func runReference(
	t *testing.T,
	ctx context.Context,
	endpoints []framework.Endpoint,
	cfg pipelineConfig,
) map[string]float64 {
	t.Helper()

	// Step 1: global affinity gate.
	candidates := referenceAffinityGate(endpoints, cfg.globalTau, cfg.affinityEpsilon, cfg.maxTTFTPenaltyMs)

	// Step 2: tier split.
	candidates = referenceTierSplit(candidates, cfg.tierEpsilon)

	// Step 3: within-tier affinity gate.
	candidates = referenceAffinityGate(candidates, cfg.localTau, cfg.affinityEpsilon, cfg.maxTTFTPenaltyMs)

	// Step 4: score.
	scorer := makeScorerPlugin(t, cfg)
	scores := scorer.Score(ctx, nil, nil, candidates)

	return namedScores(scores)
}

// ── Decomposed pipeline implementation ───────────────────────────────────────

// runDecomposed executes the actual plugin instances in pipeline order.
func runDecomposed(
	t *testing.T,
	ctx context.Context,
	endpoints []framework.Endpoint,
	cfg pipelineConfig,
) map[string]float64 {
	t.Helper()

	// Step 1: global affinity gate.
	globalFilter := makeAffinityFilter(t, cfg.globalTau, cfg.affinityEpsilon, cfg.maxTTFTPenaltyMs)
	candidates := globalFilter.Filter(ctx, nil, nil, endpoints)

	// Step 2: tier split.
	tierFilter := makeTierFilter(t, cfg.tierEpsilon)
	candidates = tierFilter.Filter(ctx, nil, nil, candidates)

	// Step 3: within-tier affinity gate.
	localFilter := makeAffinityFilter(t, cfg.localTau, cfg.affinityEpsilon, cfg.maxTTFTPenaltyMs)
	candidates = localFilter.Filter(ctx, nil, nil, candidates)

	// Step 4: score.
	scorer := makeScorerPlugin(t, cfg)
	scores := scorer.Score(ctx, nil, nil, candidates)

	return namedScores(scores)
}

// ── Plugin factories ──────────────────────────────────────────────────────────

func makeAffinityFilter(t *testing.T, tau, epsilon, maxTTFTPenaltyMs float64) *prefixaffinity.Plugin {
	t.Helper()
	// Use raw JSON to avoid omitempty dropping zero values.
	params := []byte(fmt.Sprintf(`{"affinityThreshold":%f,"explorationProbability":%f,"maxTTFTPenaltyMs":%f}`, tau, epsilon, maxTTFTPenaltyMs))
	p, err := prefixaffinity.Factory("test", params, nil)
	require.NoError(t, err)
	return p.(*prefixaffinity.Plugin)
}

func makeTierFilter(t *testing.T, epsilon float64) *sloheadroomtier.Plugin {
	t.Helper()
	params, err := json.Marshal(sloheadroomtier.Config{
		EpsilonExploreNeg: epsilon,
	})
	require.NoError(t, err)
	p, err := sloheadroomtier.Factory("test", params, nil)
	require.NoError(t, err)
	return p.(*sloheadroomtier.Plugin)
}

func makeScorerPlugin(t *testing.T, cfg pipelineConfig) *latencyscorer.Plugin {
	t.Helper()
	params, err := json.Marshal(latencyscorer.Config{
		TTFTWeight:                cfg.ttftWeight,
		TPOTWeight:                cfg.tpotWeight,
		HeadroomSelectionStrategy: cfg.strategy,
		CompositeKVWeight:         cfg.compositeKVWeight,
		CompositeQueueWeight:      cfg.compositeQueueWeight,
		CompositePrefixWeight:     cfg.compositePrefixWeight,
	})
	require.NoError(t, err)
	p, err := latencyscorer.Factory("test", params, nil)
	require.NoError(t, err)
	return p.(*latencyscorer.Plugin)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// namedScores converts map[Endpoint]float64 → map[name]float64 for comparison.
func namedScores(scores map[framework.Endpoint]float64) map[string]float64 {
	out := make(map[string]float64, len(scores))
	for ep, score := range scores {
		out[ep.GetMetadata().NamespacedName.Name] = score
	}
	return out
}

// rankByScore returns endpoint names sorted descending by score, for readable
// assertion failure messages.
func rankByScore(scores map[string]float64) []string {
	type pair struct {
		name  string
		score float64
	}
	var pairs []pair
	for name, score := range scores {
		pairs = append(pairs, pair{name, score})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].score != pairs[j].score {
			return pairs[i].score > pairs[j].score
		}
		return pairs[i].name < pairs[j].name
	})
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p.name
	}
	return out
}

// assertEquivalent checks that both pipelines produced:
// 1. The same set of scored endpoints.
// 2. The same score for each endpoint (within float tolerance).
func assertEquivalent(t *testing.T, ref, decomp map[string]float64) {
	t.Helper()
	assert.Equal(t, len(ref), len(decomp), "different number of scored endpoints")
	for name, refScore := range ref {
		decompScore, ok := decomp[name]
		assert.True(t, ok, "endpoint %q scored by reference but missing from decomposed", name)
		assert.InDelta(t, refScore, decompScore, 1e-9,
			"score mismatch for endpoint %q: reference=%.6f decomposed=%.6f", name, refScore, decompScore)
	}
	// Log ranking for debugging on failure.
	if t.Failed() {
		t.Logf("Reference ranking:   %v", rankByScore(ref))
		t.Logf("Decomposed ranking:  %v", rankByScore(decomp))
	}
}

// ── Test cases ────────────────────────────────────────────────────────────────

func TestPipelineEquivalence(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		cfg       pipelineConfig
		endpoints []endpointSpec
	}{
		// ── Positive tier, no prefix cache ──────────────────────────────────
		{
			name: "all positive, no prefix cache, strategy=least",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				{name: "a", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100},
				{name: "b", ttftHeadroom: 200, tpotHeadroom: 100, ttft: 80},
				{name: "c", ttftHeadroom: 50, tpotHeadroom: 20, ttft: 150},
			},
		},
		{
			name: "all positive, no prefix cache, strategy=most",
			cfg:  func() pipelineConfig { c := defaultConfig(); c.strategy = latencyscorer.StrategyMost; return c }(),
			endpoints: []endpointSpec{
				{name: "a", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100},
				{name: "b", ttftHeadroom: 200, tpotHeadroom: 100, ttft: 80},
				{name: "c", ttftHeadroom: 50, tpotHeadroom: 20, ttft: 150},
			},
		},

		// ── Global affinity gate fires ───────────────────────────────────────
		{
			name: "global affinity gate narrows to sticky, all positive",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				{name: "sticky1", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100, prefixMatchBlocks: 99, prefixTotalBlocks: 100},
				{name: "sticky2", ttftHeadroom: 150, tpotHeadroom: 80, ttft: 90, prefixMatchBlocks: 100, prefixTotalBlocks: 100},
				{name: "nonsticky", ttftHeadroom: 200, tpotHeadroom: 100, ttft: 80, prefixMatchBlocks: 10, prefixTotalBlocks: 100},
			},
		},
		{
			name: "global affinity gate: no sticky endpoints, all pass through",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				{name: "a", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100, prefixMatchBlocks: 50, prefixTotalBlocks: 100},
				{name: "b", ttftHeadroom: 200, tpotHeadroom: 100, ttft: 80, prefixMatchBlocks: 60, prefixTotalBlocks: 100},
			},
		},
		{
			name: "global affinity gate: TTFT load gate breaks stickiness",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				// sticky but very slow — TTFT penalty > 5000ms
				{name: "sticky-slow", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 6000, prefixMatchBlocks: 99, prefixTotalBlocks: 100},
				// non-sticky but fast
				{name: "nonsticky-fast", ttftHeadroom: 200, tpotHeadroom: 100, ttft: 100, prefixMatchBlocks: 10, prefixTotalBlocks: 100},
			},
		},
		{
			name: "global affinity gate skipped when epsilon=1",
			cfg:  func() pipelineConfig { c := defaultConfig(); c.affinityEpsilon = 1.0; return c }(),
			endpoints: []endpointSpec{
				{name: "sticky", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100, prefixMatchBlocks: 99, prefixTotalBlocks: 100},
				{name: "nonsticky", ttftHeadroom: 200, tpotHeadroom: 100, ttft: 80, prefixMatchBlocks: 10, prefixTotalBlocks: 100},
			},
		},

		// ── Tier split: positive selected ────────────────────────────────────
		{
			name: "mixed tier, epsilon=0: positive tier selected, negative ignored",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				{name: "pos1", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100},
				{name: "pos2", ttftHeadroom: 200, tpotHeadroom: 80, ttft: 90},
				{name: "neg1", ttftHeadroom: -50, tpotHeadroom: -30, ttft: 300},
				{name: "neg2", ttftHeadroom: -100, tpotHeadroom: -80, ttft: 400},
			},
		},

		// ── Tier split: negative selected ────────────────────────────────────
		{
			name: "mixed tier, epsilon=1: negative tier selected",
			cfg:  func() pipelineConfig { c := defaultConfig(); c.tierEpsilon = 1.0; return c }(),
			endpoints: []endpointSpec{
				{name: "pos1", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100},
				{name: "neg1", ttftHeadroom: -50, tpotHeadroom: -30, ttft: 300},
				{name: "neg2", ttftHeadroom: -100, tpotHeadroom: -80, ttft: 400},
			},
		},
		{
			name: "all negative, only tier: negTPOTonly wins over negTTFTonly",
			cfg:  func() pipelineConfig { c := defaultConfig(); c.tierEpsilon = 1.0; return c }(),
			endpoints: []endpointSpec{
				// negTPOTonly — should be preferred bucket
				{name: "tpot-only-a", ttftHeadroom: 50, tpotHeadroom: -20, ttft: 200},
				{name: "tpot-only-b", ttftHeadroom: 100, tpotHeadroom: -10, ttft: 180},
				// negTTFTonly — should be ignored (worse bucket)
				{name: "ttft-only", ttftHeadroom: -30, tpotHeadroom: 50, ttft: 300},
				// both neg — worst bucket
				{name: "both-neg", ttftHeadroom: -50, tpotHeadroom: -30, ttft: 400},
			},
		},
		{
			name: "all negative: negTTFTonly wins when no negTPOTonly",
			cfg:  func() pipelineConfig { c := defaultConfig(); c.tierEpsilon = 1.0; return c }(),
			endpoints: []endpointSpec{
				{name: "ttft-only-a", ttftHeadroom: -20, tpotHeadroom: 50, ttft: 200},
				{name: "ttft-only-b", ttftHeadroom: -10, tpotHeadroom: 80, ttft: 180},
				{name: "both-neg", ttftHeadroom: -50, tpotHeadroom: -30, ttft: 400},
			},
		},
		{
			name: "all negative: only negTTFTnegTPOT bucket",
			cfg:  func() pipelineConfig { c := defaultConfig(); c.tierEpsilon = 1.0; return c }(),
			endpoints: []endpointSpec{
				{name: "a", ttftHeadroom: -10, tpotHeadroom: -5, ttft: 200, dispatchedCount: 2},
				{name: "b", ttftHeadroom: -100, tpotHeadroom: -50, ttft: 400, dispatchedCount: 5},
				{name: "c", ttftHeadroom: -30, tpotHeadroom: -20, ttft: 300, dispatchedCount: 1},
			},
		},

		// ── Idle preference in negative tier ─────────────────────────────────
		{
			name: "idle endpoint wins over better-bucket busy endpoint",
			cfg:  func() pipelineConfig { c := defaultConfig(); c.tierEpsilon = 1.0; return c }(),
			endpoints: []endpointSpec{
				// idle endpoint in negTTFTnegTPOT (worst bucket) — should win
				{name: "idle-both-neg", ttftHeadroom: -50, tpotHeadroom: -30, ttft: 400, dispatchedCount: 0},
				// busy endpoint in negTPOTonly (best bucket) — should lose
				{name: "busy-tpot-only", ttftHeadroom: 50, tpotHeadroom: -5, ttft: 200, dispatchedCount: 3},
			},
		},
		{
			name: "multiple idle endpoints in negative tier: scored against each other",
			cfg:  func() pipelineConfig { c := defaultConfig(); c.tierEpsilon = 1.0; return c }(),
			endpoints: []endpointSpec{
				{name: "idle-a", ttftHeadroom: -10, tpotHeadroom: -5, ttft: 200, dispatchedCount: 0},
				{name: "idle-b", ttftHeadroom: -30, tpotHeadroom: -20, ttft: 300, dispatchedCount: 0},
				{name: "busy", ttftHeadroom: -5, tpotHeadroom: -2, ttft: 180, dispatchedCount: 3},
			},
		},

		// ── Within-tier affinity gate ─────────────────────────────────────────
		{
			name: "within-tier affinity gate narrows positive tier to sticky",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				{name: "pos-sticky", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100, prefixMatchBlocks: 85, prefixTotalBlocks: 100},
				{name: "pos-nonsticky", ttftHeadroom: 200, tpotHeadroom: 100, ttft: 80, prefixMatchBlocks: 40, prefixTotalBlocks: 100},
				// negative — filtered out by tier filter
				{name: "neg", ttftHeadroom: -50, tpotHeadroom: -30, ttft: 300, prefixMatchBlocks: 90, prefixTotalBlocks: 100},
			},
		},
		{
			name: "within-tier affinity gate: TTFT load gate breaks stickiness in positive tier",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				// sticky but overloaded within positive tier
				{name: "pos-sticky-slow", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 6000, prefixMatchBlocks: 85, prefixTotalBlocks: 100},
				// non-sticky but fast — stickiness breaks, all pass
				{name: "pos-nonsticky-fast", ttftHeadroom: 200, tpotHeadroom: 100, ttft: 100, prefixMatchBlocks: 40, prefixTotalBlocks: 100},
			},
		},

		// ── Composite fallback ────────────────────────────────────────────────
		{
			name: "no predictions: composite fallback",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				{name: "a", noPrediction: true, kvUtilPercent: 0.2, queueSize: 1, prefixMatchBlocks: 80, prefixTotalBlocks: 100},
				{name: "b", noPrediction: true, kvUtilPercent: 0.8, queueSize: 5, prefixMatchBlocks: 10, prefixTotalBlocks: 100},
				{name: "c", noPrediction: true, kvUtilPercent: 0.5, queueSize: 2, prefixMatchBlocks: 50, prefixTotalBlocks: 100},
			},
		},
		{
			name: "mixed: some predictions, noPrediction goes to negative tier",
			cfg:  func() pipelineConfig { c := defaultConfig(); c.tierEpsilon = 1.0; return c }(),
			endpoints: []endpointSpec{
				{name: "pos", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100},
				// no prediction → falls to negative tier
				{name: "nopred", noPrediction: true, kvUtilPercent: 0.3, queueSize: 1},
			},
		},

		// ── Range degeneration ────────────────────────────────────────────────
		{
			name: "all endpoints same TTFT headroom: weight collapses to TPOT only",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				{name: "a", ttftHeadroom: 100, tpotHeadroom: 200, ttft: 100},
				{name: "b", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100},
				{name: "c", ttftHeadroom: 100, tpotHeadroom: 150, ttft: 100},
			},
		},
		{
			name: "all endpoints same TPOT headroom: weight collapses to TTFT only",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				{name: "a", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100},
				{name: "b", ttftHeadroom: 200, tpotHeadroom: 50, ttft: 80},
				{name: "c", ttftHeadroom: 50, tpotHeadroom: 50, ttft: 150},
			},
		},
		{
			name: "all endpoints identical headroom: uniform scores",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				{name: "a", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100},
				{name: "b", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100},
				{name: "c", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100},
			},
		},

		// ── Edge cases ────────────────────────────────────────────────────────
		{
			name: "single endpoint: always passes through unchanged",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				{name: "only", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100},
			},
		},
		{
			name: "all positive, no SLO set: headroom always negative, all pass",
			cfg: func() pipelineConfig {
				c := defaultConfig()
				c.tierEpsilon = 0
				return c
			}(),
			endpoints: []endpointSpec{
				// When no SLO: headroom = 0 - predicted = always negative.
				// slo-headroom-tier-filter passes all in negative tier.
				{name: "a", ttftHeadroom: -100, tpotHeadroom: -50, ttft: 100},
				{name: "b", ttftHeadroom: -200, tpotHeadroom: -80, ttft: 80},
				{name: "c", ttftHeadroom: -50, tpotHeadroom: -20, ttft: 150},
			},
		},
		{
			name: "disaggregated serving: prefill endpoint has tpotHeadroom=0",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				// prefill: TPOT neutralized to 0 (TPOTHeadroom=0, treated as positive)
				{name: "prefill", ttftHeadroom: 100, tpotHeadroom: 0, ttft: 100},
				{name: "decode", ttftHeadroom: 80, tpotHeadroom: 30, ttft: 120},
				{name: "decode-overloaded", ttftHeadroom: -50, tpotHeadroom: 20, ttft: 300},
			},
		},

		// ── Additional coverage ──────────────────────────────────────────────
		{
			name: "all idle negative endpoints: scored against each other, no busy comparison",
			cfg:  func() pipelineConfig { c := defaultConfig(); c.tierEpsilon = 1.0; return c }(),
			endpoints: []endpointSpec{
				{name: "idle-a", ttftHeadroom: -10, tpotHeadroom: -5, ttft: 200, dispatchedCount: 0},
				{name: "idle-b", ttftHeadroom: -50, tpotHeadroom: -30, ttft: 300, dispatchedCount: 0},
				{name: "idle-c", ttftHeadroom: -20, tpotHeadroom: -10, ttft: 250, dispatchedCount: 0},
			},
		},
		{
			name: "strategy=most with negative tier: force-least should apply",
			cfg: func() pipelineConfig {
				c := defaultConfig()
				c.strategy = latencyscorer.StrategyMost
				c.tierEpsilon = 1.0
				return c
			}(),
			endpoints: []endpointSpec{
				{name: "a", ttftHeadroom: -10, tpotHeadroom: -5, ttft: 200, dispatchedCount: 2},
				{name: "b", ttftHeadroom: -100, tpotHeadroom: -50, ttft: 400, dispatchedCount: 5},
				{name: "c", ttftHeadroom: -30, tpotHeadroom: -20, ttft: 300, dispatchedCount: 1},
			},
		},
		{
			name: "no prefix cache attributes at all: affinity gate is no-op",
			cfg:  defaultConfig(),
			endpoints: []endpointSpec{
				{name: "a", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 100},
				{name: "b", ttftHeadroom: 200, tpotHeadroom: 100, ttft: 80},
				{name: "c", ttftHeadroom: 50, tpotHeadroom: 20, ttft: 150},
			},
		},
		{
			name: "maxTTFTPenaltyMs=0: always stick regardless of TTFT difference",
			cfg:  func() pipelineConfig { c := defaultConfig(); c.maxTTFTPenaltyMs = 0; return c }(),
			endpoints: []endpointSpec{
				{name: "sticky-slow", ttftHeadroom: 100, tpotHeadroom: 50, ttft: 99999, prefixMatchBlocks: 99, prefixTotalBlocks: 100},
				{name: "nonsticky-fast", ttftHeadroom: 200, tpotHeadroom: 100, ttft: 10, prefixMatchBlocks: 10, prefixTotalBlocks: 100},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			endpoints := make([]framework.Endpoint, len(tc.endpoints))
			for i, spec := range tc.endpoints {
				endpoints[i] = makeEndpoint(t, spec)
			}

			ref := runReference(t, ctx, endpoints, tc.cfg)
			decomp := runDecomposed(t, ctx, endpoints, tc.cfg)

			assertEquivalent(t, ref, decomp)
		})
	}
}
