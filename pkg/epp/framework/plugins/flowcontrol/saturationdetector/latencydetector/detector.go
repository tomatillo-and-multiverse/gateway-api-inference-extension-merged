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
// and representative input words. If the input-profile-tracker plugin is configured, the
// probe uses the (inputWords, prefixCacheScore) pair from the observation at the configured
// percentile of effective input. Otherwise it falls back to static config values.
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
	fwkflowcontrol "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/flowcontrol"
	fwkplugin "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/requestcontrol"
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
func LatencyDetectorFactory(name string, params json.RawMessage, handle fwkplugin.Handle) (fwkplugin.Plugin, error) {
	config := Config{
		E2ESLOMs:              DefaultE2ESLOMs,
		ProbeInputWords:       DefaultProbeInputWords,
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
	var profileProvider inputprofiletracker.InputProfileProvider
	if rawPlugin := handle.Plugin(inputprofiletracker.InputProfileTrackerType); rawPlugin != nil {
		if pp, ok := rawPlugin.(inputprofiletracker.InputProfileProvider); ok {
			profileProvider = pp
			logger.V(logutil.DEFAULT).Info("Latency detector using input-profile-tracker for dynamic probe parameters")
		}
	}

	return NewDetector(name, config, predictor, profileProvider, handle.Context(), logger), nil
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

var (
	_ framework.Filter                = &Detector{}
	_ fwkflowcontrol.SaturationDetector = &Detector{}
	_ requestcontrol.PrepareDataPlugin  = &Detector{}
)

// probeEndpoint is the minimal interface the probe needs from an endpoint.
// Both fwkdl.Endpoint and framework.Endpoint satisfy this implicitly.
type probeEndpoint interface {
	GetMetadata() *fwkdl.EndpointMetadata
	GetMetrics() *fwkdl.Metrics
}

// Detector determines system saturation based on ML-predicted latency.
type Detector struct {
	config          Config
	predictor       latencypredictor.PredictorInterface
	profileProvider inputprofiletracker.InputProfileProvider // nil = use static config
	logger          logr.Logger
	typedName       fwkplugin.TypedName

	// Cached probe results from the background goroutine.
	mu                  sync.RWMutex
	perEndpointScore    map[string]float64 // endpointID -> saturation score
	aggregateSaturation float64
	lastProbeTime       time.Time

	// The most recent set of endpoints, used by the probe goroutine.
	// Populated by PrepareRequestData (every request) or Saturation() (admission path).
	latestEndpointsMu sync.Mutex
	latestEndpoints   []probeEndpoint
}

// NewDetector creates a new latency-based saturation detector and starts the background probe loop.
func NewDetector(name string, config Config, predictor latencypredictor.PredictorInterface, profileProvider inputprofiletracker.InputProfileProvider, ctx context.Context, logger logr.Logger) *Detector {
	typedName := fwkplugin.TypedName{
		Type: LatencyDetectorType,
		Name: name,
	}
	logger = logger.WithName(typedName.String())

	if config.isStreaming() {
		logger.V(logutil.DEFAULT).Info("Creating new LatencySaturationDetector (streaming)",
			"ttftSLOMs", config.TTFTSLOMs,
			"tpotSLOMs", config.TPOTSLOMs,
			"probeInputWords", config.ProbeInputWords,
			"probePrefixCacheScore", config.ProbePrefixCacheScore,
			"probeInterval", config.ProbeInterval,
			"headroom", config.Headroom)
	} else {
		logger.V(logutil.DEFAULT).Info("Creating new LatencySaturationDetector (non-streaming, E2E)",
			"e2eSLOMs", config.E2ESLOMs,
			"probeInputWords", config.ProbeInputWords,
			"probePrefixCacheScore", config.ProbePrefixCacheScore,
			"probeInterval", config.ProbeInterval,
			"headroom", config.Headroom)
	}

	d := &Detector{
		config:              config,
		predictor:           predictor,
		profileProvider:     profileProvider,
		logger:              logger,
		typedName:           typedName,
		perEndpointScore:    make(map[string]float64),
		aggregateSaturation: 1.0, // Conservative default until first probe completes.
	}

	go d.probeLoop(ctx)
	return d
}

// TypedName returns the type and name tuple of this plugin instance.
func (d *Detector) TypedName() fwkplugin.TypedName {
	return d.typedName
}

// Saturation returns the pool-level saturation signal based on cached probe results.
func (d *Detector) Saturation(_ context.Context, candidatePods []fwkdl.Endpoint) float64 {
	eps := make([]probeEndpoint, len(candidatePods))
	for i, p := range candidatePods {
		eps[i] = p
	}
	d.latestEndpointsMu.Lock()
	d.latestEndpoints = eps
	d.latestEndpointsMu.Unlock()

	if len(candidatePods) == 0 {
		return 1.0
	}

	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.aggregateSaturation
}

// PrepareRequestData snapshots the current endpoints so the background probe has
// endpoints to work with, even when Saturation() is not being called.
func (d *Detector) PrepareRequestData(_ context.Context, _ *framework.LLMRequest, endpoints []framework.Endpoint) error {
	eps := make([]probeEndpoint, len(endpoints))
	for i, ep := range endpoints {
		eps[i] = ep
	}
	d.latestEndpointsMu.Lock()
	d.latestEndpoints = eps
	d.latestEndpointsMu.Unlock()
	return nil
}

// Produces returns nil — this plugin doesn't produce endpoint attributes.
func (d *Detector) Produces() map[string]any { return nil }

// Consumes returns nil — endpoint snapshot doesn't depend on other attributes.
func (d *Detector) Consumes() map[string]any { return nil }

// Filter removes endpoints whose per-request predicted latency exceeds the SLO.
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
	// The tracker returns the (inputWords, prefixCacheScore) pair from the observation
	// at the configured percentile of effective input, preserving both original features.
	probeInputWords := d.config.ProbeInputWords
	probePrefixCache := d.config.ProbePrefixCacheScore
	if d.profileProvider != nil {
		words, cache := d.profileProvider.ProbeProfile(d.config.ProbeInputWords, d.config.ProbePrefixCacheScore)
		if words > 0 {
			probeInputWords = words
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
			InputTokenLength:   probeInputWords, // "InputTokenLength" is the sidecar field name; we send word count
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
		"aggregateSaturation", aggregate,
		"probeInputWords", probeInputWords,
		"probePrefixCache", probePrefixCache)
}

// computeEndpointSaturation computes saturation for a single endpoint from predicted latency.
func (d *Detector) computeEndpointSaturation(predictedTTFT, predictedTPOT float64) float64 {
	activeSLO := d.config.activeTTFTSLO()
	if activeSLO <= 0 {
		return 0.0
	}

	primarySaturation := predictedTTFT / activeSLO

	if !d.config.isStreaming() || d.config.TPOTSLOMs <= 0 {
		return primarySaturation
	}

	tpotSaturation := predictedTPOT / d.config.TPOTSLOMs
	return max(primarySaturation, tpotSaturation)
}
