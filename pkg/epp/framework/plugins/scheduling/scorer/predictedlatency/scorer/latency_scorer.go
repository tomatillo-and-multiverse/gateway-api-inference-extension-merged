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

package scorer

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/observability/logging"
	fwkplugin "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrlatency "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/latency"
	attrprefix "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

const (
	LatencyScorerType = "latency-scorer"
	eps               = 1e-9
	wMax              = 100
	minWeight         = 1
)

// compile-time validation
var _ framework.Scorer = &LatencyScorer{}

// LatencyScorerConfig holds configuration for the latency scorer.
type LatencyScorerConfig struct {
	// TTFTWeight controls the relative importance of TTFT headroom.
	TTFTWeight float64 `json:"ttftWeight,omitempty"`
	// TPOTWeight controls the relative importance of TPOT headroom.
	TPOTWeight float64 `json:"tpotWeight,omitempty"`

	// NegHeadroomTTFTWeight controls TTFT weight in negative headroom blending.
	NegHeadroomTTFTWeight float64 `json:"negHeadroomTTFTWeight,omitempty"`
	// NegHeadroomTPOTWeight controls TPOT weight in negative headroom blending.
	NegHeadroomTPOTWeight float64 `json:"negHeadroomTPOTWeight,omitempty"`

	// EpsilonExploreNeg is the probability of scoring negative headroom endpoints
	// instead of positive (1% = explore negative tier). Range: [0, 1].
	EpsilonExploreNeg float64 `json:"epsilonExploreNeg,omitempty"`

	// AffinityGateTau is the prefix cache score threshold for the within-tier
	// affinity gate. Endpoints with prefix score >= Tau are preferred within
	// each tier (positive/negative). Applied BEFORE normalization so that
	// headroom scores are computed relative to the sticky subset only.
	// Set to 0 to disable the within-tier affinity gate.
	AffinityGateTau float64 `json:"affinityGateTau,omitempty"`

	// EpsilonExploreSticky is the probability of ignoring the within-tier
	// affinity gate (exploration). Range: [0, 1].
	EpsilonExploreSticky float64 `json:"epsilonExploreSticky,omitempty"`

	// AffinityMaxTTFTPenaltyMs is the maximum TTFT penalty (ms) before
	// breaking stickiness in the within-tier affinity gate.
	// Set to 0 to disable the TTFT load gate.
	AffinityMaxTTFTPenaltyMs float64 `json:"affinityMaxTTFTPenaltyMs,omitempty"`

	// CompositeKVWeight is the weight for KV cache free ratio in composite scoring.
	CompositeKVWeight float64 `json:"compositeKVWeight,omitempty"`
	// CompositeQueueWeight is the weight for queue availability in composite scoring.
	CompositeQueueWeight float64 `json:"compositeQueueWeight,omitempty"`
	// CompositePrefixWeight is the weight for prefix cache score in composite scoring.
	CompositePrefixWeight float64 `json:"compositePrefixWeight,omitempty"`
}

var LatencyScorerDefaultConfig = LatencyScorerConfig{
	TTFTWeight:               0.8,
	TPOTWeight:               0.2,
	NegHeadroomTTFTWeight:    0.8,
	NegHeadroomTPOTWeight:    0.2,
	EpsilonExploreNeg:        0.01,
	AffinityGateTau:          0.80,
	EpsilonExploreSticky:     0.01,
	AffinityMaxTTFTPenaltyMs: 5000.0,
	CompositeKVWeight:        1,
	CompositeQueueWeight:     1,
	CompositePrefixWeight:    1,
}

// LatencyScorer scores endpoints based on predicted latency headroom.
//
// Scoring model:
//   - 99% of the time: only positive headroom endpoints get non-zero scores
//   - 1% of the time: only negative headroom endpoints get non-zero scores
//   - Within each tier, a local affinity gate (tau=0.80) filters to sticky pods
//   - Positive: scores based on normalized blended headroom
//   - Negative: strict idle-pod preference, then hierarchical deficit buckets
//   - No predictions: composite fallback (KV + queue + prefix)
type LatencyScorer struct {
	typedName fwkplugin.TypedName
	config    LatencyScorerConfig
}

// LatencyScorerFactory creates a new LatencyScorer plugin.
func LatencyScorerFactory(name string, rawParameters json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	config := LatencyScorerDefaultConfig
	if len(rawParameters) > 0 {
		if err := json.Unmarshal(rawParameters, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config for LatencyScorer: %w", err)
		}
	}
	return NewLatencyScorer(config).WithName(name), nil
}

func NewLatencyScorer(config LatencyScorerConfig) *LatencyScorer {
	return &LatencyScorer{
		typedName: fwkplugin.TypedName{Type: LatencyScorerType, Name: LatencyScorerType},
		config:    config,
	}
}

func (s *LatencyScorer) WithName(name string) *LatencyScorer {
	s.typedName.Name = name
	return s
}

func (s *LatencyScorer) TypedName() fwkplugin.TypedName {
	return s.typedName
}

func (s *LatencyScorer) Category() framework.ScorerCategory {
	return framework.Balance
}

func (s *LatencyScorer) Consumes() map[string]any {
	return map[string]any{
		attrlatency.LatencyPredictionInfoKey: attrlatency.LatencyPredictionInfo{},
	}
}

// epData holds per-endpoint prediction data gathered from attributes.
type epData struct {
	endpoint         framework.Endpoint
	info             *attrlatency.LatencyPredictionInfo
	ttftHeadroom     float64
	tpotHeadroom     float64
	positive         bool // both headrooms >= 0
	prefixCacheScore float64
}

// Score returns a float64 score in [0,1] for each endpoint.
func (s *LatencyScorer) Score(ctx context.Context, _ *framework.CycleState, _ *framework.LLMRequest, endpoints []framework.Endpoint) map[framework.Endpoint]float64 {
	logger := log.FromContext(ctx)
	scores := make(map[framework.Endpoint]float64, len(endpoints))
	for _, ep := range endpoints {
		scores[ep] = 0
	}

	// Gather prediction data from endpoint attributes.
	data := make([]epData, 0, len(endpoints))
	hasPredictions := false

	for _, ep := range endpoints {
		d := epData{endpoint: ep, prefixCacheScore: prefixCacheScore(ep)}
		if raw, ok := ep.Get(attrlatency.LatencyPredictionInfoKey); ok {
			d.info = raw.(*attrlatency.LatencyPredictionInfo)
			d.ttftHeadroom = d.info.TTFTHeadroom()
			d.tpotHeadroom = d.info.TPOTHeadroom()
			d.positive = d.ttftHeadroom >= 0 && d.tpotHeadroom >= 0
			hasPredictions = true
		}
		data = append(data, d)
	}

	// Fallback: no predictions — use composite scoring.
	if !hasPredictions {
		logger.V(logutil.DEBUG).Info("LatencyScorer: no predictions, using composite fallback")
		return s.compositeScores(ctx, endpoints)
	}

	// Split into positive and negative headroom cohorts.
	var positive, negative []epData
	for _, d := range data {
		if d.info == nil {
			negative = append(negative, d)
		} else if d.positive {
			positive = append(positive, d)
		} else {
			negative = append(negative, d)
		}
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Tiered selection: 99% positive, 1% negative.
	switch {
	case len(positive) > 0 && len(negative) > 0:
		if rng.Float64() < s.config.EpsilonExploreNeg {
			logger.V(logutil.DEBUG).Info("LatencyScorer: scoring negative tier (epsilon explore)")
			s.scoreNegativeTier(ctx, negative, scores, rng)
		} else {
			logger.V(logutil.DEBUG).Info("LatencyScorer: scoring positive tier")
			s.scorePositiveTier(ctx, positive, scores, rng)
		}
	case len(positive) > 0:
		s.scorePositiveTier(ctx, positive, scores, rng)
	case len(negative) > 0:
		s.scoreNegativeTier(ctx, negative, scores, rng)
	}

	return scores
}

// applyWithinTierAffinityGate narrows candidates to sticky endpoints within a tier.
// Applied BEFORE normalization so headroom scores are relative to the sticky subset.
// Matches the old monolith's per-tier epsilon-greedy affinity gating.
func (s *LatencyScorer) applyWithinTierAffinityGate(ctx context.Context, data []epData, rng *rand.Rand, tierLabel string) []epData {
	if s.config.AffinityGateTau <= 0 {
		return data
	}

	logger := log.FromContext(ctx)

	sticky := make([]epData, 0, len(data))
	for _, d := range data {
		if d.prefixCacheScore >= s.config.AffinityGateTau {
			sticky = append(sticky, d)
		}
	}

	if len(sticky) == 0 {
		return data
	}

	// Epsilon-greedy exploration.
	if rng.Float64() < s.config.EpsilonExploreSticky {
		logger.V(logutil.DEBUG).Info("LatencyScorer: within-tier affinity gate exploring",
			"tier", tierLabel, "epsilon", s.config.EpsilonExploreSticky, "sticky", len(sticky))
		return data
	}

	// TTFT load gate.
	if s.config.AffinityMaxTTFTPenaltyMs > 0 {
		bestAll := math.MaxFloat64
		for _, d := range data {
			if d.info != nil && d.info.TTFT() > 0 && d.info.TTFT() < bestAll {
				bestAll = d.info.TTFT()
			}
		}
		bestSticky := math.MaxFloat64
		for _, d := range sticky {
			if d.info != nil && d.info.TTFT() > 0 && d.info.TTFT() < bestSticky {
				bestSticky = d.info.TTFT()
			}
		}
		if bestAll < math.MaxFloat64 && bestSticky < math.MaxFloat64 {
			penalty := bestSticky - bestAll
			if penalty > s.config.AffinityMaxTTFTPenaltyMs {
				logger.V(logutil.DEBUG).Info("LatencyScorer: within-tier TTFT penalty too high",
					"tier", tierLabel, "penalty", penalty, "max", s.config.AffinityMaxTTFTPenaltyMs)
				return data
			}
		}
	}

	logger.V(logutil.DEBUG).Info("LatencyScorer: within-tier affinity gate applied",
		"tier", tierLabel, "sticky", len(sticky), "total", len(data))
	return sticky
}

// scorePositiveTier scores positive headroom endpoints.
func (s *LatencyScorer) scorePositiveTier(ctx context.Context, data []epData, scores map[framework.Endpoint]float64, rng *rand.Rand) {
	// Apply within-tier affinity gate BEFORE normalization.
	data = s.applyWithinTierAffinityGate(ctx, data, rng, "positive")

	// Find min/max headroom for normalization.
	minTTFTH, maxTTFTH := math.MaxFloat64, -math.MaxFloat64
	minTPOTH, maxTPOTH := math.MaxFloat64, -math.MaxFloat64
	for _, d := range data {
		if d.ttftHeadroom < minTTFTH {
			minTTFTH = d.ttftHeadroom
		}
		if d.ttftHeadroom > maxTTFTH {
			maxTTFTH = d.ttftHeadroom
		}
		if d.tpotHeadroom < minTPOTH {
			minTPOTH = d.tpotHeadroom
		}
		if d.tpotHeadroom > maxTPOTH {
			maxTPOTH = d.tpotHeadroom
		}
	}

	ttftRange := maxTTFTH - minTTFTH
	tpotRange := maxTPOTH - minTPOTH
	alpha, beta := normalizedWeights(s.config.TTFTWeight, s.config.TPOTWeight)

	logger := log.FromContext(ctx)
	for _, d := range data {
		nTTFT := 0.5
		if ttftRange > eps {
			nTTFT = (d.ttftHeadroom - minTTFTH) / (ttftRange + eps)
		}
		nTPOT := 0.5
		if tpotRange > eps {
			nTPOT = (d.tpotHeadroom - minTPOTH) / (tpotRange + eps)
		}

		// Combined headroom: 0 = tightest, 1 = most headroom.
		combined := alpha*nTTFT + beta*nTPOT

		// Weight in [minWeight+1, wMax].
		w := float64(int((1.0-combined)*float64(wMax-minWeight)) + minWeight + 1)
		scores[d.endpoint] = w / float64(wMax)

		logger.V(logutil.TRACE).Info("LatencyScorer: positive",
			"endpoint", d.endpoint.GetMetadata().NamespacedName.Name,
			"ttftH", d.ttftHeadroom, "tpotH", d.tpotHeadroom,
			"combined", combined, "weight", w)
	}
}

// scoreNegativeTier scores negative headroom endpoints.
func (s *LatencyScorer) scoreNegativeTier(ctx context.Context, data []epData, scores map[framework.Endpoint]float64, rng *rand.Rand) {
	logger := log.FromContext(ctx)

	// Apply within-tier affinity gate BEFORE idle split and normalization.
	data = s.applyWithinTierAffinityGate(ctx, data, rng, "negative")

	// Step 1: Strict idle-pod preference.
	var idle, busy []epData
	for _, d := range data {
		if d.endpoint.GetMetrics().RunningRequestsSize == 0 {
			idle = append(idle, d)
		} else {
			busy = append(busy, d)
		}
	}

	var selected []epData
	if len(idle) > 0 {
		logger.V(logutil.DEBUG).Info("LatencyScorer: preferring idle pods in negative tier", "idle", len(idle))
		selected = idle
	} else {
		selected = busy
	}

	// Step 2: Hierarchical deficit buckets.
	var negTTFTnegTPOT, negTTFTnonNegTPOT, nonNegTTFTnegTPOT, rest []epData
	for _, d := range selected {
		ttftNeg := d.ttftHeadroom < 0
		tpotNeg := d.tpotHeadroom < 0
		switch {
		case ttftNeg && tpotNeg:
			negTTFTnegTPOT = append(negTTFTnegTPOT, d)
		case ttftNeg && !tpotNeg:
			negTTFTnonNegTPOT = append(negTTFTnonNegTPOT, d)
		case !ttftNeg && tpotNeg:
			nonNegTTFTnegTPOT = append(nonNegTTFTnegTPOT, d)
		default:
			rest = append(rest, d)
		}
	}

	alpha, beta := normalizedWeights(s.config.NegHeadroomTTFTWeight, s.config.NegHeadroomTPOTWeight)

	const wRange = 80
	s.scoreDeficitBucket(ctx, negTTFTnegTPOT, scores, alpha, beta, wRange)
	s.scoreDeficitBucket(ctx, negTTFTnonNegTPOT, scores, alpha, beta, wRange)
	s.scoreDeficitBucket(ctx, nonNegTTFTnegTPOT, scores, alpha, beta, wRange)

	// Edge case bucket: minimal weight.
	for _, d := range rest {
		scores[d.endpoint] = float64(minWeight) / float64(wMax)
	}
}

// scoreDeficitBucket scores endpoints within a deficit bucket using blended weighting.
func (s *LatencyScorer) scoreDeficitBucket(ctx context.Context, data []epData, scores map[framework.Endpoint]float64, alpha, beta float64, wRange int) {
	if len(data) == 0 {
		return
	}

	type deficit struct {
		d       epData
		ttftDef float64
		tpotDef float64
	}
	defs := make([]deficit, 0, len(data))
	minTTFT, maxTTFT := math.MaxFloat64, -math.MaxFloat64
	minTPOT, maxTPOT := math.MaxFloat64, -math.MaxFloat64

	for _, d := range data {
		dd := deficit{d: d}
		if d.ttftHeadroom < 0 {
			dd.ttftDef = -d.ttftHeadroom
		}
		if d.tpotHeadroom < 0 {
			dd.tpotDef = -d.tpotHeadroom
		}
		defs = append(defs, dd)
		if dd.ttftDef < minTTFT {
			minTTFT = dd.ttftDef
		}
		if dd.ttftDef > maxTTFT {
			maxTTFT = dd.ttftDef
		}
		if dd.tpotDef < minTPOT {
			minTPOT = dd.tpotDef
		}
		if dd.tpotDef > maxTPOT {
			maxTPOT = dd.tpotDef
		}
	}

	ttftRange := maxTTFT - minTTFT
	tpotRange := maxTPOT - minTPOT

	logger := log.FromContext(ctx)
	for _, dd := range defs {
		nTTFT := 0.0
		if ttftRange > eps {
			nTTFT = (dd.ttftDef - minTTFT) / (ttftRange + eps)
		}
		nTPOT := 0.0
		if tpotRange > eps {
			nTPOT = (dd.tpotDef - minTPOT) / (tpotRange + eps)
		}

		blended := alpha*nTTFT + beta*nTPOT
		w := int((1.0-blended)*float64(wRange)) + minWeight + 1

		scores[dd.d.endpoint] = float64(w) / float64(wMax)
		logger.V(logutil.TRACE).Info("LatencyScorer: negative deficit",
			"endpoint", dd.d.endpoint.GetMetadata().NamespacedName.Name,
			"ttftDef", dd.ttftDef, "tpotDef", dd.tpotDef,
			"blended", blended, "weight", w)
	}
}

// compositeScores returns scores based on KV cache, queue, and prefix cache
// when latency predictions are unavailable.
func (s *LatencyScorer) compositeScores(ctx context.Context, endpoints []framework.Endpoint) map[framework.Endpoint]float64 {
	scores := make(map[framework.Endpoint]float64, len(endpoints))

	wkv, wq, wpref := s.config.CompositeKVWeight, s.config.CompositeQueueWeight, s.config.CompositePrefixWeight
	sumw := wkv + wq + wpref
	if sumw <= 0 {
		wkv, wq, wpref = 1, 0, 0
		sumw = 1
	}
	wkv /= sumw
	wq /= sumw
	wpref /= sumw

	minQ, maxQ := math.MaxInt32, 0
	for _, ep := range endpoints {
		q := ep.GetMetrics().WaitingQueueSize
		if q < minQ {
			minQ = q
		}
		if q > maxQ {
			maxQ = q
		}
	}
	qRange := float64(maxQ - minQ)

	logger := log.FromContext(ctx)
	for _, ep := range endpoints {
		q := ep.GetMetrics().WaitingQueueSize
		relQueue := 1.0
		if qRange > 0 {
			relQueue = float64(maxQ-q) / qRange
		}

		kvFree := 1.0 - ep.GetMetrics().KVCacheUsagePercent
		prefix := prefixCacheScore(ep)

		composite := wkv*kvFree + wq*relQueue + wpref*prefix
		w := int(math.Round(float64(minWeight) + float64(wMax-minWeight)*composite))
		score := float64(w) / float64(wMax)

		scores[ep] = score
		logger.V(logutil.TRACE).Info("LatencyScorer: composite",
			"endpoint", ep.GetMetadata().NamespacedName.Name,
			"kvFree", kvFree, "relQueue", relQueue, "prefix", prefix, "score", score)
	}

	return scores
}

func normalizedWeights(a, b float64) (float64, float64) {
	sum := a + b
	if sum <= 0 {
		return 1.0, 0.0
	}
	return a / sum, b / sum
}

// prefixCacheScore returns the prefix cache score for an endpoint.
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
