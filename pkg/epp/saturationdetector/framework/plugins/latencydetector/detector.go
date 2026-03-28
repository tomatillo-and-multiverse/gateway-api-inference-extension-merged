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

// Package latencydetector implements a saturation detector that uses ML-predicted latency
// from the latency predictor sidecar to determine endpoint saturation.
//
// # Non-Streaming vs Streaming
//
// The mode is determined by which SLO field is set:
//
//   - E2ESLOMs set (non-streaming): The predictor's TTFT output represents E2E request
//     latency. TPOT is ignored entirely.
//
//   - TTFTSLOMs set (streaming): TTFT = time to first token, TPOT = time per output token.
//     Both are checked independently against their respective SLOs.
//
// # Background Probing (for Saturation signal)
//
// A background goroutine periodically probes each endpoint by constructing a synthetic
// prediction request using the endpoint's current metrics (queue depth, KV cache utilization)
// and a configurable representative input token count.
//
// Non-streaming:
//
//	EndpointSaturation = PredictedE2ELatency / E2ESLOMs
//
// Streaming:
//
//	EndpointSaturation = Max(PredictedTTFT / TTFTSLOMs, PredictedTPOT / TPOTSLOMs)
//
// In both cases: PoolSaturation = Average(EndpointSaturation)
//
// # Per-Request Filtering (for Scheduling)
//
// During scheduling, the Filter uses LatencyPredictionInfo already stored in endpoint
// attributes by the latency predictor plugin's PrepareRequestData phase. Endpoints whose
// predicted latency exceeds the SLO (with optional headroom) are filtered out.
// In non-streaming mode, only TTFT headroom (= E2E headroom) is checked.
package latencydetector

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/observability/logging"
	eppmetrics "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/metrics"
	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	fwkplugin "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrlatency "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/latency"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/requestcontrol/requestdataproducer/inputprofiletracker"
	latencypredictor "sigs.k8s.io/gateway-api-inference-extension/sidecars/latencypredictorasync"
)

const (
	loggerName = "LatencySaturationDetector"

	// LatencyDetectorType is the unique identifier for this plugin.
	LatencyDetectorType = "latency-detector"
)

// LatencyDetectorFactory creates a new latency detector plugin from config.
func LatencyDetectorFactory(_ string, params json.RawMessage, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	config := Config{
		E2ESLOMs:              DefaultE2ESLOMs,
		ProbeInputTokenLength: DefaultProbeInputTokenLength,
		ProbePrefixCacheScore: DefaultProbePrefixCacheScore,
		ProbeInterval:         DefaultProbeInterval,
		Headroom:              DefaultHeadroom,
	}
	if len(params) > 0 {
		if err := json.Unmarshal(params, &config); err != nil {
			return nil, fmt.Errorf("failed to unmarshal latency detector config: %w", err)
		}
	}
	if err := config.validate(); err != nil {
		return nil, fmt.Errorf("invalid latency detector config: %w", err)
	}

	logger := log.FromContext(handle.Context())

	predictor, err := startPredictor(handle, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to start latency predictor for saturation detector: %w", err)
	}

	// Look up the input profile tracker for dynamic probe parameters.
	// If not configured, the detector falls back to static config values.
	var profileProvider inputprofiletracker.InputProfileProvider
	if rawPlugin := handle.Plugin(inputprofiletracker.InputProfileTrackerType); rawPlugin != nil {
		if pp, ok := rawPlugin.(inputprofiletracker.InputProfileProvider); ok {
			profileProvider = pp
			logger.V(logutil.DEFAULT).Info("Latency detector using input-profile-tracker for dynamic probe parameters")
		}
	}

	return NewDetector(config, predictor, profileProvider, handle.Context(), logger), nil
}

func startPredictor(handle fwkplugin.Handle, logger logr.Logger) (latencypredictor.PredictorInterface, error) {
	predictor := latencypredictor.New(latencypredictor.ConfigFromEnv(), ctrl.Log.WithName("latency-saturation-detector"))
	if err := predictor.Start(handle.Context()); err != nil {
		return nil, fmt.Errorf("failed to start predictor: %w", err)
	}

	go func() {
		<-handle.Context().Done()
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		predictor.Stop(stopCtx)
		logger.V(logutil.DEFAULT).Info("Latency saturation detector predictor stopped")
	}()

	return predictor, nil
}

var _ framework.Filter = &Detector{}

// Detector determines system saturation based on ML-predicted latency.
type Detector struct {
	config          Config
	predictor       latencypredictor.PredictorInterface
	profileProvider inputprofiletracker.InputProfileProvider // nil = use static config
	logger          logr.Logger

	// Cached probe results from the background goroutine.
	mu                  sync.RWMutex
	perEndpointScore    map[string]float64 // endpointID -> saturation score
	aggregateSaturation float64
	lastProbeTime       time.Time

	// The most recent set of endpoints seen by Saturation(), used by the probe goroutine.
	latestEndpointsMu sync.Mutex
	latestEndpoints   []fwkdl.Endpoint
}

// NewDetector creates a new latency-based saturation detector and starts the background probe loop.
// profileProvider may be nil, in which case static config values are used for probing.
func NewDetector(config Config, predictor latencypredictor.PredictorInterface, profileProvider inputprofiletracker.InputProfileProvider, ctx context.Context, logger logr.Logger) *Detector {
	logger = logger.WithName(loggerName)
	if config.isStreaming() {
		logger.V(logutil.DEFAULT).Info("Creating new LatencySaturationDetector (streaming)",
			"ttftSLOMs", config.TTFTSLOMs,
			"tpotSLOMs", config.TPOTSLOMs,
			"probeInputTokenLength", config.ProbeInputTokenLength,
			"probePrefixCacheScore", config.ProbePrefixCacheScore,
			"probeInterval", config.ProbeInterval,
			"headroom", config.Headroom)
	} else {
		logger.V(logutil.DEFAULT).Info("Creating new LatencySaturationDetector (non-streaming, E2E)",
			"e2eSLOMs", config.E2ESLOMs,
			"probeInputTokenLength", config.ProbeInputTokenLength,
			"probePrefixCacheScore", config.ProbePrefixCacheScore,
			"probeInterval", config.ProbeInterval,
			"headroom", config.Headroom)
	}

	d := &Detector{
		config:              config,
		predictor:           predictor,
		profileProvider:     profileProvider,
		logger:              logger,
		perEndpointScore:    make(map[string]float64),
		aggregateSaturation: 1.0, // Conservative default until first probe completes.
	}

	go d.probeLoop(ctx)
	return d
}

// TypedName returns the type and name tuple of this plugin instance.
func (d *Detector) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{
		Type: LatencyDetectorType,
		Name: LatencyDetectorType,
	}
}

// Saturation returns the pool-level saturation signal based on cached probe results.
//
// It snapshots the provided endpoints for the next background probe cycle, then returns
// the cached aggregate saturation score. Values:
//   - < 1.0: pool has headroom
//   - >= 1.0: pool is at or above SLO capacity
func (d *Detector) Saturation(_ context.Context, candidatePods []fwkdl.Endpoint) float64 {
	// Snapshot the latest endpoints for the background probe.
	d.latestEndpointsMu.Lock()
	d.latestEndpoints = candidatePods
	d.latestEndpointsMu.Unlock()

	if len(candidatePods) == 0 {
		return 1.0
	}

	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.aggregateSaturation
}

// Filter removes endpoints whose per-request predicted latency exceeds the SLO.
//
// It reads LatencyPredictionInfo from endpoint attributes (populated by the latency predictor
// plugin during PrepareRequestData). In non-streaming mode (E2ESLOMs), only TTFT headroom
// (= E2E headroom) is checked and TPOT is ignored. In streaming mode (TTFTSLOMs), both
// TTFT and TPOT headroom are checked when TPOTSLOMs > 0.
//
// If all endpoints would be filtered, returns all endpoints (fail-open at pool level).
func (d *Detector) Filter(
	_ context.Context,
	_ *framework.CycleState,
	_ *framework.LLMRequest,
	endpoints []framework.Endpoint,
) []framework.Endpoint {
	activeSLO := d.config.activeTTFTSLO()
	if activeSLO <= 0 {
		return endpoints
	}

	// TTFT headroom from the predictor represents E2E headroom in non-streaming mode.
	ttftHeadroomLimit := -activeSLO * d.config.Headroom
	checkTPOT := d.config.isStreaming() && d.config.TPOTSLOMs > 0
	tpotHeadroomLimit := 0.0
	if checkTPOT {
		tpotHeadroomLimit = -d.config.TPOTSLOMs * d.config.Headroom
	}

	filtered := make([]framework.Endpoint, 0, len(endpoints))
	for _, endpoint := range endpoints {
		raw, ok := endpoint.Get(attrlatency.LatencyPredictionInfoKey)
		if !ok {
			// No prediction info available — pass through (fail open).
			filtered = append(filtered, endpoint)
			continue
		}

		info, ok := raw.(*attrlatency.LatencyPredictionInfo)
		if !ok || info == nil {
			filtered = append(filtered, endpoint)
			continue
		}

		ttftOk := info.TTFTHeadroom() >= ttftHeadroomLimit
		tpotOk := true
		if checkTPOT {
			tpotOk = info.TPOTHeadroom() >= tpotHeadroomLimit
		}

		if ttftOk && tpotOk {
			filtered = append(filtered, endpoint)
		}
	}

	// Fail open: if everything was filtered, return all endpoints.
	if len(filtered) == 0 {
		return endpoints
	}
	return filtered
}

// probeLoop runs the background probe cycle on the configured interval.
func (d *Detector) probeLoop(ctx context.Context) {
	ticker := time.NewTicker(d.config.probeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.probe(ctx)
		}
	}
}

// probe sends synthetic prediction requests to all known endpoints and updates the cache.
func (d *Detector) probe(ctx context.Context) {
	d.latestEndpointsMu.Lock()
	endpoints := d.latestEndpoints
	d.latestEndpointsMu.Unlock()

	if len(endpoints) == 0 {
		d.mu.Lock()
		d.aggregateSaturation = 1.0
		d.lastProbeTime = time.Now()
		d.mu.Unlock()
		return
	}

	// Use dynamic values from the input profile tracker if available.
	// The tracker returns the (inputTokens, prefixCacheScore) pair from the observation
	// at the p90 of effective input, preserving both original features for the sidecar model.
	probeInputTokens := d.config.ProbeInputTokenLength
	probePrefixCache := d.config.ProbePrefixCacheScore
	if d.profileProvider != nil {
		tokens, cache := d.profileProvider.ProbeProfile(d.config.ProbeInputTokenLength, d.config.ProbePrefixCacheScore)
		if tokens > 0 {
			probeInputTokens = tokens
			probePrefixCache = cache
		}
	}

	requests := make([]latencypredictor.PredictionRequest, 0, len(endpoints))
	endpointIDs := make([]string, 0, len(endpoints))

	for _, ep := range endpoints {
		metrics := ep.GetMetrics()
		metadata := ep.GetMetadata()
		if metrics == nil || metadata == nil {
			continue
		}

		req := latencypredictor.PredictionRequest{
			KVCachePercentage:  metrics.KVCacheUsagePercent,
			InputTokenLength:   probeInputTokens,
			NumRequestWaiting:  metrics.WaitingQueueSize,
			NumRequestRunning:  metrics.RunningRequestsSize,
			NumTokensGenerated: 1,
			PrefixCacheScore:   probePrefixCache,
		}
		requests = append(requests, req)
		endpointIDs = append(endpointIDs, metadata.NamespacedName.String())
	}

	if len(requests) == 0 {
		d.mu.Lock()
		d.aggregateSaturation = 1.0
		d.lastProbeTime = time.Now()
		d.mu.Unlock()
		return
	}

	resp, err := d.predictor.PredictBulkStrict(ctx, requests)
	if err != nil {
		d.logger.V(logutil.DEFAULT).Error(err, "Probe prediction failed, keeping cached saturation")
		return
	}

	if len(resp.Predictions) != len(endpointIDs) {
		d.logger.V(logutil.DEFAULT).Info("Probe returned mismatched prediction count",
			"expected", len(endpointIDs), "got", len(resp.Predictions))
		return
	}

	newScores := make(map[string]float64, len(endpointIDs))
	var totalScore float64

	for i, pred := range resp.Predictions {
		score := d.computeEndpointSaturation(pred.TTFT, pred.TPOT)
		newScores[endpointIDs[i]] = score
		totalScore += score

		d.logger.V(logutil.DEBUG).Info("Probe result",
			"endpoint", endpointIDs[i],
			"predictedTTFT", pred.TTFT,
			"predictedTPOT", pred.TPOT,
			"saturation", score)
	}

	aggregate := totalScore / float64(len(endpointIDs))

	d.mu.Lock()
	d.perEndpointScore = newScores
	d.aggregateSaturation = aggregate
	d.lastProbeTime = time.Now()
	d.mu.Unlock()

	// Emit per-endpoint and aggregate saturation metrics.
	for ep, score := range newScores {
		eppmetrics.RecordLatencyDetectorEndpointSaturation(ep, score)
	}
	eppmetrics.RecordLatencyDetectorPoolSaturation(aggregate)

	d.logger.V(logutil.DEFAULT).Info("Probe cycle complete",
		"endpoints", len(endpointIDs),
		"aggregateSaturation", aggregate)
}

// computeEndpointSaturation computes saturation for a single endpoint from predicted latency.
//
// Non-streaming (E2ESLOMs):
//
//	saturation = predictedE2ELatency / E2ESLOMs
//
// Streaming (TTFTSLOMs):
//
//	saturation = max(predictedTTFT / TTFTSLOMs, predictedTPOT / TPOTSLOMs)
func (d *Detector) computeEndpointSaturation(predictedTTFT, predictedTPOT float64) float64 {
	activeSLO := d.config.activeTTFTSLO()
	if activeSLO <= 0 {
		return 0.0
	}

	// In non-streaming mode, predictedTTFT is the E2E latency.
	// In streaming mode, it is the time to first token.
	primarySaturation := predictedTTFT / activeSLO

	// TPOT is only meaningful in streaming mode where per-token latency is trained.
	if !d.config.isStreaming() || d.config.TPOTSLOMs <= 0 {
		return primarySaturation
	}

	tpotSaturation := predictedTPOT / d.config.TPOTSLOMs
	return max(primarySaturation, tpotSaturation)
}
