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
)

func makeEndpoint(name string) framework.Endpoint {
	return framework.NewEndpoint(
		&fwkdl.EndpointMetadata{NamespacedName: k8stypes.NamespacedName{Name: name}},
		&fwkdl.Metrics{UpdateTime: time.Now()},
		nil,
	)
}

func TestTracker_ProbeProfile_Fallback(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples: 100, Percentile: 90,
	})

	tokens, cache := tracker.ProbeProfile(512, 0.1)
	require.Equal(t, 512, tokens)
	require.InDelta(t, 0.1, cache, 1e-6)
}

func TestTracker_ProbeProfile_SelectsByEffectiveInput(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples: 100, Percentile: 50,
	})

	now := time.Now()
	// Obs 1: 1000 tokens, 0.9 cache → effective = 100
	// Obs 2: 200 tokens, 0.0 cache → effective = 200
	// Obs 3: 500 tokens, 0.5 cache → effective = 250
	// Sorted: [100, 200, 250]. p50 index = 1 → obs 2.
	tracker.record(observation{timestamp: now, inputTokens: 1000, prefixCacheScore: 0.9, effectiveInputTokens: 100})
	tracker.record(observation{timestamp: now, inputTokens: 200, prefixCacheScore: 0.0, effectiveInputTokens: 200})
	tracker.record(observation{timestamp: now, inputTokens: 500, prefixCacheScore: 0.5, effectiveInputTokens: 250})

	tokens, cache := tracker.ProbeProfile(0, 0)
	require.Equal(t, 200, tokens)
	require.InDelta(t, 0.0, cache, 1e-6)
}

func TestTracker_ProbeProfile_P90(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples: 200, Percentile: 90,
	})

	now := time.Now()
	for i := 1; i <= 100; i++ {
		eff := i * 10
		tracker.record(observation{
			timestamp: now, inputTokens: eff * 2,
			prefixCacheScore: 0.5, effectiveInputTokens: eff,
		})
	}

	tokens, cache := tracker.ProbeProfile(0, 0)
	// p90 index = 90 → effective=910 → inputTokens=1820, cache=0.5.
	require.Equal(t, 1820, tokens)
	require.InDelta(t, 0.5, cache, 1e-6)
}

func TestTracker_WindowExpiry(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "100ms", windowDuration: 100 * time.Millisecond,
		MaxSamples: 100, Percentile: 90,
	})

	tracker.record(observation{
		timestamp: time.Now().Add(-200 * time.Millisecond),
		inputTokens: 999, prefixCacheScore: 0.5, effectiveInputTokens: 500,
	})

	tokens, cache := tracker.ProbeProfile(42, 0.1)
	require.Equal(t, 42, tokens)
	require.InDelta(t, 0.1, cache, 1e-6)
}

func TestTracker_RingBuffer_Overflow(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples: 5, Percentile: 90,
	})

	now := time.Now()
	for i := 1; i <= 8; i++ {
		tracker.record(observation{
			timestamp: now, inputTokens: i * 100,
			prefixCacheScore: 0, effectiveInputTokens: i * 100,
		})
	}

	require.Equal(t, 5, len(tracker.observations))
	tokens, _ := tracker.ProbeProfile(0, 0)
	// Buffer: [400,500,600,700,800]. p90 index=4 → 800.
	require.Equal(t, 800, tokens)
}

func TestTracker_PrepareRequestData(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples: 100, Percentile: 50,
	})

	request := &framework.LLMRequest{
		Body: &framework.LLMRequestBody{
			Completions: &framework.CompletionsRequest{
				Prompt: "the quick brown fox jumps over the lazy dog",
			},
		},
	}

	ep := makeEndpoint("pod1")
	ep.Put(attrprefix.PrefixCacheMatchInfoKey, attrprefix.NewPrefixCacheMatchInfo(3, 10, 16))

	err := tracker.PrepareRequestData(context.Background(), request, []framework.Endpoint{ep})
	require.NoError(t, err)

	// Word count = 9, prefix cache = 3/10 = 0.3, effective = round(9 * 0.7) = 6.
	tokens, cache := tracker.ProbeProfile(0, 0)
	require.Equal(t, 9, tokens)
	require.InDelta(t, 0.3, cache, 1e-6)

	// Verify attribute on endpoint.
	raw, ok := ep.Get(attrinputprofile.InputProfileInfoKey)
	require.True(t, ok)
	info := raw.(*attrinputprofile.InputProfileInfo)
	require.Equal(t, 9, info.InputTokens())
	require.InDelta(t, 0.3, info.PrefixCacheScore(), 1e-6)
	require.Equal(t, 6, info.EffectiveInputTokens())
}

func TestTracker_PrepareRequestData_NoCacheInfo(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples: 100, Percentile: 50,
	})

	request := &framework.LLMRequest{
		Body: &framework.LLMRequestBody{
			Completions: &framework.CompletionsRequest{
				Prompt: "one two three four five",
			},
		},
	}

	ep := makeEndpoint("pod1")
	err := tracker.PrepareRequestData(context.Background(), request, []framework.Endpoint{ep})
	require.NoError(t, err)

	tokens, cache := tracker.ProbeProfile(0, 0)
	require.Equal(t, 5, tokens)
	require.InDelta(t, 0.0, cache, 1e-6)
}

func TestTracker_PrepareRequestData_NoBody(t *testing.T) {
	tracker := NewTracker(Config{
		WindowDuration: "5m", windowDuration: 5 * time.Minute,
		MaxSamples: 100, Percentile: 50,
	})

	err := tracker.PrepareRequestData(context.Background(), &framework.LLMRequest{}, []framework.Endpoint{makeEndpoint("pod1")})
	require.NoError(t, err)

	// No body → nothing recorded → fallback.
	tokens, _ := tracker.ProbeProfile(42, 0)
	require.Equal(t, 42, tokens)
}

func TestCountInputTokens(t *testing.T) {
	request := &framework.LLMRequest{
		Body: &framework.LLMRequestBody{
			Completions: &framework.CompletionsRequest{Prompt: "one two three"},
		},
	}
	require.Equal(t, 3, countInputTokens(request))
	require.Equal(t, 0, countInputTokens(&framework.LLMRequest{}))
	require.Equal(t, 0, countInputTokens(nil))
}

func TestTracker_TypedName(t *testing.T) {
	tracker := NewTracker(Config{})
	tn := tracker.TypedName()
	require.Equal(t, InputProfileTrackerType, tn.Type)
}

func TestPercentileIndex(t *testing.T) {
	require.Equal(t, 0, percentileIndex(1, 90))
	require.Equal(t, 9, percentileIndex(10, 90))
	require.Equal(t, 0, percentileIndex(10, 0))
	require.Equal(t, 9, percentileIndex(10, 100))
	require.Equal(t, 5, percentileIndex(10, 50))
}
