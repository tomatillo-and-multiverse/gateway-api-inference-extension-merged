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

package predictor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"

	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	latencypredictor "sigs.k8s.io/gateway-api-inference-extension/sidecars/latencypredictorasync"
)

// PredictedLatency is the latency data provider plugin. It handles:
//   - PrepareRequestData: bulk predictions via the latency predictor sidecar
//   - PreRequest: dispatch-time bookkeeping (token counters, request queues)
//   - ResponseHeader/ResponseBody: training data collection (TTFT/TPOT)
//   - Produces/Consumes: endpoint attribute declarations
//
// Scoring, picking, and admission are handled by separate sub-plugins:
// LatencyScorer, AffinityWeightedPicker, and LatencyAdmission.
type PredictedLatency struct {
	typedName             plugin.TypedName
	latencypredictor      latencypredictor.PredictorInterface
	runningRequestLists   sync.Map                                      // Key: types.NamespacedName, Value: *requestPriorityQueue
	sloContextStore       *ttlcache.Cache[string, *predictedLatencyCtx] // TTL cache for request contexts
	config                Config
	prefillTokensInFlight sync.Map // Key: pod NamespacedName.String(), Value: *atomic.Int64
}

// podCounter returns the atomic counter for the given pod key, creating it if necessary.
func (t *PredictedLatency) podCounter(m *sync.Map, key string) *atomic.Int64 {
	v, _ := m.LoadOrStore(key, new(atomic.Int64))
	return v.(*atomic.Int64)
}

type Config struct {
	SamplingMean          float64       `json:"samplingMean,omitempty"`
	MaxSampledTokens      int           `json:"maxSampledTokens,omitempty"`
	SLOBufferFactor       float64       `json:"sloBufferFactor,omitempty"`
	AffinityGateTauGlobal float64       `json:"affinityGateTauGlobal,omitempty"`
	ContextTTL            time.Duration `json:"contextTTL,omitempty"`
	StreamingMode         bool          `json:"streamingMode,omitempty"`
	EndpointRoleLabel     string        `json:"endpointRoleLabel,omitempty"`
}

var DefaultConfig = Config{
	SamplingMean:          1000,
	MaxSampledTokens:      0,
	SLOBufferFactor:       1,
	AffinityGateTauGlobal: 0.99,
	ContextTTL:            5 * time.Minute,
	StreamingMode:         true,
}

func PredictedLatencyFactory(name string, rawParameters json.RawMessage, handle plugin.Handle) (plugin.Plugin, error) {
	parameters := DefaultConfig
	if len(rawParameters) > 0 {
		if err := json.Unmarshal(rawParameters, &parameters); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config for PredictedLatency: %w", err)
		}
	}

	if err := parameters.validate(); err != nil {
		return nil, fmt.Errorf("invalid PredictedLatency config: %w", err)
	}

	predictor, err := startPredictor(handle)
	if err != nil {
		return nil, fmt.Errorf("failed to start latency predictor: %w", err)
	}

	return NewPredictedLatency(parameters, predictor).WithName(name), nil
}

func (c *Config) validate() error {
	var errs []error

	if c.SamplingMean <= 0 {
		errs = append(errs, fmt.Errorf("samplingMean must be > 0, got %f", c.SamplingMean))
	}

	if c.MaxSampledTokens < 0 {
		errs = append(errs, fmt.Errorf("maxSampledTokens must be >= 0, got %d", c.MaxSampledTokens))
	}

	if c.SLOBufferFactor <= 0 {
		errs = append(errs, fmt.Errorf("sloBufferFactor must be > 0, got %f", c.SLOBufferFactor))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func NewPredictedLatency(config Config, predictor latencypredictor.PredictorInterface) *PredictedLatency {
	predictedLatency := &PredictedLatency{
		typedName:        plugin.TypedName{Type: LatencyDataProviderPluginType, Name: LatencyDataProviderPluginType},
		latencypredictor: predictor,
		config:           config,
	}

	predictedLatency.sloContextStore = ttlcache.New(
		ttlcache.WithTTL[string, *predictedLatencyCtx](config.ContextTTL),
	)

	predictedLatency.sloContextStore.OnEviction(func(ctx context.Context, reason ttlcache.EvictionReason, item *ttlcache.Item[string, *predictedLatencyCtx]) {
		if reason != ttlcache.EvictionReasonExpired {
			return
		}
		plCtx := item.Value()
		predictedLatency.removeRequestFromQueue(item.Key(), plCtx)
		if plCtx.prefillTokensAtDispatch > 0 || plCtx.prefillTokensAtDispatchOnPrefill > 0 {
			if plCtx.prefillTargetMetadata != nil && plCtx.ttft == 0 {
				prefillPodKey := plCtx.prefillTargetMetadata.NamespacedName.String()
				if predictedLatency.podCounter(&predictedLatency.prefillTokensInFlight, prefillPodKey).Add(-int64(plCtx.inputTokenCount)) == 0 {
					predictedLatency.prefillTokensInFlight.Delete(prefillPodKey)
				}
			}
			if plCtx.targetMetadata != nil {
				decodePodKey := plCtx.targetMetadata.NamespacedName.String()
				if predictedLatency.podCounter(&predictedLatency.prefillTokensInFlight, decodePodKey).Add(-int64(plCtx.inputTokenCount)) == 0 {
					predictedLatency.prefillTokensInFlight.Delete(decodePodKey)
				}
			}
		}
	})

	go predictedLatency.sloContextStore.Start()
	return predictedLatency
}

func startPredictor(handle plugin.Handle) (latencypredictor.PredictorInterface, error) {
	predictor := latencypredictor.New(latencypredictor.ConfigFromEnv(), ctrl.Log.WithName("latency-predictor"))
	if err := predictor.Start(handle.Context()); err != nil {
		return nil, fmt.Errorf("failed to start latency predictor: %w", err)
	}

	go func() {
		<-handle.Context().Done()
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		predictor.Stop(stopCtx)
	}()
	return predictor, nil
}

func (s *PredictedLatency) TypedName() plugin.TypedName {
	return s.typedName
}

func (s *PredictedLatency) WithName(name string) *PredictedLatency {
	s.typedName.Name = name
	return s
}

func (t *PredictedLatency) getOrMakePredictedLatencyContextForRequest(request *framework.LLMRequest) *predictedLatencyCtx {
	sloCtx, err := t.getPredictedLatencyContextForRequest(request)
	if err != nil {
		sloCtx = newPredictedLatencyContext(request)
	}

	return sloCtx
}
