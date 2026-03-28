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

package inputprofiletracker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrinputprofile "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/inputprofile"
	attrprefix "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	attrreqinput "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/requestinput"
)

func makeEndpoint(name string) framework.Endpoint {
	return framework.NewEndpoint(
		&fwkdl.EndpointMetadata{NamespacedName: k8stypes.NamespacedName{Name: name}},
		&fwkdl.Metrics{UpdateTime: time.Now()},
		nil,
	)
}

// setRequestInput simulates request-input-producer having run before the tracker.
func setRequestInput(ep framework.Endpoint, tokenCount int) {
	ep.Put(attrreqinput.RequestInputInfoKey, attrreqinput.NewRequestInputInfo(tokenCount))
}

func TestTracker_ProbeProfile_Fallback(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples:     100,
		Percentile:     90,
	})

	// No observations → return fallback.
	tokens, cache := tracker.ProbeProfile(512, 0.1)
	require.Equal(t, 512, tokens)
	require.InDelta(t, 0.1, cache, 1e-6)
}

func TestTracker_ProbeProfile_SelectsByEffectiveInput(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples:     100,
		Percentile:     50, // Median for easier reasoning.
	})

	now := time.Now()
	// Record observations with varying effective input.
	// Obs 1: 1000 tokens, 0.9 cache → effective = 100
	// Obs 2: 200 tokens, 0.0 cache → effective = 200
	// Obs 3: 500 tokens, 0.5 cache → effective = 250
	// Sorted by effective: [100, 200, 250]. p50 index = 1 → effective=200, which is obs 2.
	tracker.record(observation{timestamp: now, inputTokens: 1000, prefixCacheScore: 0.9, effectiveInputTokens: 100})
	tracker.record(observation{timestamp: now, inputTokens: 200, prefixCacheScore: 0.0, effectiveInputTokens: 200})
	tracker.record(observation{timestamp: now, inputTokens: 500, prefixCacheScore: 0.5, effectiveInputTokens: 250})

	tokens, cache := tracker.ProbeProfile(0, 0)
	// Should return the obs at p50 of effective input (obs 2).
	require.Equal(t, 200, tokens)
	require.InDelta(t, 0.0, cache, 1e-6)
}

func TestTracker_ProbeProfile_P90(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples:     200,
		Percentile:     90,
	})

	now := time.Now()
	// Record 100 observations: effective = 10, 20, ..., 1000.
	for i := 1; i <= 100; i++ {
		eff := i * 10
		tracker.record(observation{
			timestamp:            now,
			inputTokens:          eff * 2, // 2x effective (50% cache)
			prefixCacheScore:     0.5,
			effectiveInputTokens: eff,
		})
	}

	tokens, cache := tracker.ProbeProfile(0, 0)
	// p90 index = (90 * 100) / 100 = 90 → effective=910 → inputTokens=1820, cache=0.5.
	require.Equal(t, 1820, tokens)
	require.InDelta(t, 0.5, cache, 1e-6)
}

func TestTracker_WindowExpiry(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "100ms", windowDuration: 100 * time.Millisecond,
		MaxSamples:     100,
		Percentile:     90,
	})

	// Record old observation.
	tracker.record(observation{
		timestamp:            time.Now().Add(-200 * time.Millisecond),
		inputTokens:          999,
		prefixCacheScore:     0.5,
		effectiveInputTokens: 500,
	})

	// Expired → fallback.
	tokens, cache := tracker.ProbeProfile(42, 0.1)
	require.Equal(t, 42, tokens)
	require.InDelta(t, 0.1, cache, 1e-6)
}

func TestTracker_RingBuffer_Overflow(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples:     5,
		Percentile:     90,
	})

	now := time.Now()
	// Write 8 entries into a buffer of size 5.
	for i := 1; i <= 8; i++ {
		tracker.record(observation{
			timestamp:            now,
			inputTokens:          i * 100,
			prefixCacheScore:     0,
			effectiveInputTokens: i * 100,
		})
	}

	// Buffer should contain entries 4,5,6,7,8 (oldest evicted).
	require.Equal(t, 5, len(tracker.observations))
	tokens, _ := tracker.ProbeProfile(0, 0)
	// Sorted effective: [400, 500, 600, 700, 800]. p90 index = 4 → 800.
	require.Equal(t, 800, tokens)
}

func TestTracker_PrepareRequestData(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples:     100,
		Percentile:     50,
	})

	// Create an endpoint with RequestInputInfo (from request-input-producer) and prefix cache info.
	ep := makeEndpoint("pod1")
	setRequestInput(ep, 9) // 9 words
	ep.Put(attrprefix.PrefixCacheMatchInfoKey, attrprefix.NewPrefixCacheMatchInfo(3, 10, 16))

	err := tracker.PrepareRequestData(context.Background(), nil, []framework.Endpoint{ep})
	require.NoError(t, err)

	// 9 tokens, prefix cache = 0.3, effective = round(9 * 0.7) = 6.
	tokens, cache := tracker.ProbeProfile(0, 0)
	require.Equal(t, 9, tokens)
	require.InDelta(t, 0.3, cache, 1e-6)

	// Verify the InputProfileInfo attribute was set on the endpoint.
	raw, ok := ep.Get(attrinputprofile.InputProfileInfoKey)
	require.True(t, ok, "InputProfileInfo attribute should be set on endpoint")
	info := raw.(*attrinputprofile.InputProfileInfo)
	require.Equal(t, 9, info.InputTokens())
	require.InDelta(t, 0.3, info.PrefixCacheScore(), 1e-6)
	require.Equal(t, 6, info.EffectiveInputTokens())
}

func TestTracker_PrepareRequestData_NoCacheInfo(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples:     100,
		Percentile:     50,
	})

	ep := makeEndpoint("pod1")
	setRequestInput(ep, 500)

	err := tracker.PrepareRequestData(context.Background(), nil, []framework.Endpoint{ep})
	require.NoError(t, err)

	// No prefix cache → cache score = 0, effective = 500.
	tokens, cache := tracker.ProbeProfile(0, 0)
	require.Equal(t, 500, tokens)
	require.InDelta(t, 0.0, cache, 1e-6)
}

func TestTracker_PrepareRequestData_NoInputInfo(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples:     100,
		Percentile:     50,
	})

	// No RequestInputInfo on endpoint → nothing recorded.
	ep := makeEndpoint("pod1")
	err := tracker.PrepareRequestData(context.Background(), nil, []framework.Endpoint{ep})
	require.NoError(t, err)

	tokens, _ := tracker.ProbeProfile(42, 0)
	require.Equal(t, 42, tokens) // Fallback.
}

func TestTracker_TypedName(t *testing.T) {
	tracker := NewTracker(Config{})
	tn := tracker.TypedName()
	require.Equal(t, InputProfileTrackerType, tn.Type)
	require.Equal(t, InputProfileTrackerType, tn.Name)
}

func TestReadInputTokenCount(t *testing.T) {
	ep := makeEndpoint("pod1")
	setRequestInput(ep, 42)

	require.Equal(t, 42, readInputTokenCount([]framework.Endpoint{ep}))
	require.Equal(t, 0, readInputTokenCount([]framework.Endpoint{makeEndpoint("pod2")}))
	require.Equal(t, 0, readInputTokenCount(nil))
}

func TestPercentileIndex(t *testing.T) {
	require.Equal(t, 0, percentileIndex(1, 90))
	require.Equal(t, 9, percentileIndex(10, 90))
	require.Equal(t, 0, percentileIndex(10, 0))
	require.Equal(t, 9, percentileIndex(10, 100))
	require.Equal(t, 5, percentileIndex(10, 50))
}
