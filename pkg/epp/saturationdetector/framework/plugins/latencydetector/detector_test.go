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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	backendmetrics "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/backend/metrics"
	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrlatency "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/latency"
	latencypredictor "sigs.k8s.io/gateway-api-inference-extension/sidecars/latencypredictorasync"
)

// --- Test Helpers ---

func makeDLEndpoint(name string, queueDepth, runningReqs int, kvUsage float64) *backendmetrics.FakePodMetrics {
	return &backendmetrics.FakePodMetrics{
		Metadata: &fwkdl.EndpointMetadata{
			NamespacedName: k8stypes.NamespacedName{Name: name, Namespace: "ns1"},
		},
		Metrics: &fwkdl.Metrics{
			WaitingQueueSize:    queueDepth,
			RunningRequestsSize: runningReqs,
			KVCacheUsagePercent: kvUsage,
			UpdateTime:          time.Now(),
		},
	}
}

func makeSchedulingEndpoint(name string) framework.Endpoint {
	return framework.NewEndpoint(
		&fwkdl.EndpointMetadata{NamespacedName: k8stypes.NamespacedName{Name: name, Namespace: "ns1"}},
		&fwkdl.Metrics{UpdateTime: time.Now()},
		nil,
	)
}

func setLatencyInfo(ep framework.Endpoint, ttftHeadroom, tpotHeadroom, ttft, tpot float64) {
	ttftValid := ttftHeadroom >= 0
	tpotValid := tpotHeadroom >= 0
	ep.Put(attrlatency.LatencyPredictionInfoKey,
		attrlatency.NewLatencyPredictionInfo(ttftValid, tpotValid, ttftHeadroom, tpotHeadroom, ttft, tpot))
}

// fakePredictor implements PredictorInterface for testing the probe loop.
type fakePredictor struct {
	mu          sync.Mutex
	predictions []latencypredictor.PredictionResponse
	err         error
	callCount   int
}

func (f *fakePredictor) Predict(_ context.Context, _ latencypredictor.PredictionRequest) (*latencypredictor.PredictionResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakePredictor) PredictBulk(_ context.Context, _ []latencypredictor.PredictionRequest) (*latencypredictor.BulkPredictionResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakePredictor) PredictBulkStrict(_ context.Context, reqs []latencypredictor.PredictionRequest) (*latencypredictor.BulkPredictionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.callCount++

	if f.err != nil {
		return nil, f.err
	}

	preds := f.predictions
	if len(preds) != len(reqs) {
		return nil, fmt.Errorf("prediction count mismatch: have %d, want %d", len(preds), len(reqs))
	}

	return &latencypredictor.BulkPredictionResponse{
		Predictions:           preds,
		TotalRequests:         len(reqs),
		SuccessfulPredictions: len(reqs),
	}, nil
}

func (f *fakePredictor) AddTrainingDataBulk(_ []latencypredictor.TrainingEntry) error {
	return nil
}

func (f *fakePredictor) getCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.callCount
}

// --- Tests ---

func TestComputeEndpointSaturation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		config    Config
		ttft      float64
		tpot      float64
		wantScore float64
	}{
		{
			name:      "Non-streaming - E2E under SLO",
			config:    Config{E2ESLOMs: 200},
			ttft:      100,
			tpot:      50,
			wantScore: 0.5, // 100/200, TPOT ignored
		},
		{
			name:      "Non-streaming - E2E at SLO",
			config:    Config{E2ESLOMs: 200},
			ttft:      200,
			tpot:      50,
			wantScore: 1.0, // 200/200
		},
		{
			name:      "Non-streaming - E2E over SLO",
			config:    Config{E2ESLOMs: 200},
			ttft:      400,
			tpot:      50,
			wantScore: 2.0, // 400/200
		},
		{
			name:      "Non-streaming - TPOT ignored",
			config:    Config{E2ESLOMs: 200},
			ttft:      100,
			tpot:      200, // Would be 4.0 if TPOT were checked
			wantScore: 0.5, // Only 100/200, TPOT ignored in non-streaming
		},
		{
			name:      "Streaming - TTFT dominates",
			config:    Config{TTFTSLOMs: 200, TPOTSLOMs: 50},
			ttft:      300,
			tpot:      25,
			wantScore: 1.5, // max(300/200, 25/50) = max(1.5, 0.5) = 1.5
		},
		{
			name:      "Streaming - TPOT dominates",
			config:    Config{TTFTSLOMs: 200, TPOTSLOMs: 50},
			ttft:      100,
			tpot:      100,
			wantScore: 2.0, // max(100/200, 100/50) = max(0.5, 2.0) = 2.0
		},
		{
			name:      "Streaming - no TPOTSLOMs, only TTFT",
			config:    Config{TTFTSLOMs: 200, TPOTSLOMs: 0},
			ttft:      150,
			tpot:      100,
			wantScore: 0.75, // 150/200, TPOT ignored because TPOTSLOMs=0
		},
		{
			name:      "Zero SLO returns 0",
			config:    Config{E2ESLOMs: 0},
			ttft:      100,
			tpot:      50,
			wantScore: 0.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := &Detector{config: tc.config}
			got := d.computeEndpointSaturation(tc.ttft, tc.tpot)
			require.InDelta(t, tc.wantScore, got, 1e-6)
		})
	}
}

func TestDetector_Saturation_EmptyPods(t *testing.T) {
	t.Parallel()

	d := &Detector{
		config:              Config{E2ESLOMs: 200},
		perEndpointScore:    make(map[string]float64),
		aggregateSaturation: 0.5,
	}

	got := d.Saturation(context.Background(), []fwkdl.Endpoint{})
	require.Equal(t, 1.0, got, "Empty pods should return 1.0")
}

func TestDetector_Saturation_ReturnsCachedValue(t *testing.T) {
	t.Parallel()

	d := &Detector{
		config:              Config{E2ESLOMs: 200},
		perEndpointScore:    map[string]float64{"ns1/pod1": 0.5},
		aggregateSaturation: 0.5,
	}

	pods := []fwkdl.Endpoint{makeDLEndpoint("pod1", 2, 5, 0.3)}
	got := d.Saturation(context.Background(), pods)
	require.InDelta(t, 0.5, got, 1e-6, "Should return cached aggregate saturation")
}

func TestDetector_Probe(t *testing.T) {
	t.Parallel()

	predictor := &fakePredictor{
		predictions: []latencypredictor.PredictionResponse{
			{TTFT: 100, TPOT: 20},
			{TTFT: 300, TPOT: 30},
		},
	}

	d := &Detector{
		config: Config{
			TTFTSLOMs:             200,
			TPOTSLOMs:             50,
			ProbeInputTokenLength: 512,
			ProbePrefixCacheScore: 0,
		},
		predictor:           predictor,
		logger:              logr.Discard(),
		perEndpointScore:    make(map[string]float64),
		aggregateSaturation: 1.0,
	}

	// Set up endpoints.
	d.latestEndpoints = []fwkdl.Endpoint{
		makeDLEndpoint("pod1", 2, 5, 0.3),
		makeDLEndpoint("pod2", 8, 10, 0.7),
	}

	d.probe(context.Background())

	require.Equal(t, 1, predictor.getCallCount())

	d.mu.RLock()
	defer d.mu.RUnlock()

	// Streaming: pod1: max(100/200, 20/50) = max(0.5, 0.4) = 0.5
	require.InDelta(t, 0.5, d.perEndpointScore["ns1/pod1"], 1e-6)
	// Streaming: pod2: max(300/200, 30/50) = max(1.5, 0.6) = 1.5
	require.InDelta(t, 1.5, d.perEndpointScore["ns1/pod2"], 1e-6)
	// aggregate: (0.5 + 1.5) / 2 = 1.0
	require.InDelta(t, 1.0, d.aggregateSaturation, 1e-6)
}

func TestDetector_Probe_Error_KeepsCachedValue(t *testing.T) {
	t.Parallel()

	predictor := &fakePredictor{
		err: fmt.Errorf("sidecar unavailable"),
	}

	d := &Detector{
		config: Config{
			E2ESLOMs:              200,
			ProbeInputTokenLength: 512,
		},
		predictor:           predictor,
		logger:              logr.Discard(),
		perEndpointScore:    map[string]float64{"ns1/pod1": 0.3},
		aggregateSaturation: 0.3,
	}
	d.latestEndpoints = []fwkdl.Endpoint{makeDLEndpoint("pod1", 1, 3, 0.2)}

	d.probe(context.Background())

	d.mu.RLock()
	defer d.mu.RUnlock()

	require.InDelta(t, 0.3, d.aggregateSaturation, 1e-6, "Should keep cached value on error")
}

func TestDetector_Probe_NilMetrics(t *testing.T) {
	t.Parallel()

	predictor := &fakePredictor{}

	d := &Detector{
		config: Config{
			E2ESLOMs:              200,
			ProbeInputTokenLength: 512,
		},
		predictor:           predictor,
		logger:              logr.Discard(),
		perEndpointScore:    make(map[string]float64),
		aggregateSaturation: 1.0,
	}

	// Endpoint with nil metrics.
	d.latestEndpoints = []fwkdl.Endpoint{
		&backendmetrics.FakePodMetrics{
			Metadata: &fwkdl.EndpointMetadata{
				NamespacedName: k8stypes.NamespacedName{Name: "pod1", Namespace: "ns1"},
			},
			Metrics: nil,
		},
	}

	d.probe(context.Background())

	d.mu.RLock()
	defer d.mu.RUnlock()

	// No valid endpoints to probe → aggregate stays at 1.0.
	require.InDelta(t, 1.0, d.aggregateSaturation, 1e-6)
	require.Equal(t, 0, predictor.getCallCount(), "Should not call predictor with no valid endpoints")
}

func TestDetector_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		config    Config
		endpoints func() []framework.Endpoint
		wantLen   int
	}{
		{
			name:   "All pass - positive E2E headroom",
			config: Config{E2ESLOMs: 200},
			endpoints: func() []framework.Endpoint {
				ep1 := makeSchedulingEndpoint("pod1")
				setLatencyInfo(ep1, 50, 10, 150, 40)
				ep2 := makeSchedulingEndpoint("pod2")
				setLatencyInfo(ep2, 100, 20, 100, 30)
				return []framework.Endpoint{ep1, ep2}
			},
			wantLen: 2,
		},
		{
			name:   "One filtered - negative E2E headroom",
			config: Config{E2ESLOMs: 200},
			endpoints: func() []framework.Endpoint {
				epGood := makeSchedulingEndpoint("pod1")
				setLatencyInfo(epGood, 50, 10, 150, 40)
				epBad := makeSchedulingEndpoint("pod2")
				setLatencyInfo(epBad, -30, 10, 230, 40) // E2E exceeds SLO
				return []framework.Endpoint{epGood, epBad}
			},
			wantLen: 1,
		},
		{
			name:   "Streaming - one filtered by negative TPOT headroom",
			config: Config{TTFTSLOMs: 200, TPOTSLOMs: 50},
			endpoints: func() []framework.Endpoint {
				epGood := makeSchedulingEndpoint("pod1")
				setLatencyInfo(epGood, 50, 10, 150, 40)
				epBad := makeSchedulingEndpoint("pod2")
				setLatencyInfo(epBad, 50, -10, 150, 60) // TPOT exceeds SLO
				return []framework.Endpoint{epGood, epBad}
			},
			wantLen: 1,
		},
		{
			name:   "Non-streaming - TPOT ignored",
			config: Config{E2ESLOMs: 200},
			endpoints: func() []framework.Endpoint {
				ep := makeSchedulingEndpoint("pod1")
				setLatencyInfo(ep, 50, -100, 150, 200) // Bad TPOT but non-streaming
				return []framework.Endpoint{ep}
			},
			wantLen: 1,
		},
		{
			name:   "Streaming - TPOT ignored when TPOTSLOMs is 0",
			config: Config{TTFTSLOMs: 200, TPOTSLOMs: 0},
			endpoints: func() []framework.Endpoint {
				ep := makeSchedulingEndpoint("pod1")
				setLatencyInfo(ep, 50, -100, 150, 200) // Bad TPOT but no TPOT SLO configured
				return []framework.Endpoint{ep}
			},
			wantLen: 1,
		},
		{
			name:   "Headroom allows burst",
			config: Config{E2ESLOMs: 200, Headroom: 0.2}, // Allows up to -40ms headroom
			endpoints: func() []framework.Endpoint {
				ep1 := makeSchedulingEndpoint("pod1")
				setLatencyInfo(ep1, -30, 10, 230, 40) // -30 > -40 → passes
				ep2 := makeSchedulingEndpoint("pod2")
				setLatencyInfo(ep2, -50, 10, 250, 40) // -50 < -40 → filtered
				return []framework.Endpoint{ep1, ep2}
			},
			wantLen: 1,
		},
		{
			name:   "No prediction info - pass through",
			config: Config{E2ESLOMs: 200},
			endpoints: func() []framework.Endpoint {
				return []framework.Endpoint{makeSchedulingEndpoint("pod1")}
			},
			wantLen: 1,
		},
		{
			name:   "All filtered - fail open",
			config: Config{E2ESLOMs: 200},
			endpoints: func() []framework.Endpoint {
				ep1 := makeSchedulingEndpoint("pod1")
				setLatencyInfo(ep1, -50, 10, 250, 40)
				ep2 := makeSchedulingEndpoint("pod2")
				setLatencyInfo(ep2, -100, 10, 300, 40)
				return []framework.Endpoint{ep1, ep2}
			},
			wantLen: 2, // Fail open returns all.
		},
		{
			name:   "Zero SLO - no filtering",
			config: Config{E2ESLOMs: 0},
			endpoints: func() []framework.Endpoint {
				ep := makeSchedulingEndpoint("pod1")
				setLatencyInfo(ep, -100, -100, 300, 200)
				return []framework.Endpoint{ep}
			},
			wantLen: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := &Detector{config: tc.config, logger: logr.Discard()}
			got := d.Filter(context.Background(), nil, nil, tc.endpoints())
			require.Len(t, got, tc.wantLen)
		})
	}
}

func TestDetector_ProbeLoop_Integration(t *testing.T) {
	t.Parallel()

	predictor := &fakePredictor{
		predictions: []latencypredictor.PredictionResponse{
			{TTFT: 80, TPOT: 20},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := &Detector{
		config: Config{
			E2ESLOMs:              200,
			ProbeInputTokenLength: 512,
			ProbeInterval:         "50ms",
			probeInterval:         50 * time.Millisecond, // Fast for testing.
		},
		predictor:           predictor,
		logger:              logr.Discard(),
		perEndpointScore:    make(map[string]float64),
		aggregateSaturation: 1.0,
	}

	// Set endpoints before starting the loop.
	d.latestEndpoints = []fwkdl.Endpoint{makeDLEndpoint("pod1", 1, 3, 0.2)}

	go d.probeLoop(ctx)

	// Wait for at least one probe cycle.
	require.Eventually(t, func() bool {
		return predictor.getCallCount() >= 1
	}, 2*time.Second, 10*time.Millisecond)

	d.mu.RLock()
	sat := d.aggregateSaturation
	d.mu.RUnlock()

	// pod1: 80/200 = 0.4
	require.InDelta(t, 0.4, sat, 1e-6)

	cancel()
}

func TestDetector_TypedName(t *testing.T) {
	d := &Detector{}
	tn := d.TypedName()
	require.Equal(t, LatencyDetectorType, tn.Type)
	require.Equal(t, LatencyDetectorType, tn.Name)
}

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name:   "Valid non-streaming",
			config: Config{E2ESLOMs: 200},
		},
		{
			name:   "Valid streaming TTFT only",
			config: Config{TTFTSLOMs: 200},
		},
		{
			name:   "Valid streaming TTFT + TPOT",
			config: Config{TTFTSLOMs: 200, TPOTSLOMs: 50},
		},
		{
			name:    "Invalid - both E2E and TTFT set",
			config:  Config{E2ESLOMs: 200, TTFTSLOMs: 200},
			wantErr: true,
		},
		{
			name:    "Invalid - neither set",
			config:  Config{},
			wantErr: true,
		},
		{
			name:    "Invalid - TPOT without TTFT (non-streaming)",
			config:  Config{E2ESLOMs: 200, TPOTSLOMs: 50},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.validate()
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
