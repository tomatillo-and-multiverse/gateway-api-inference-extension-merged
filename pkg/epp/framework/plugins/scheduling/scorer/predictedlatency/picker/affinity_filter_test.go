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
	"testing"

	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrlatency "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/latency"
	attrprefix "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/prefix"
)

func makePickerEndpoint(name string, score float64) *framework.ScoredEndpoint {
	ep := framework.NewEndpoint(
		&fwkdl.EndpointMetadata{NamespacedName: k8stypes.NamespacedName{Name: name}},
		&fwkdl.Metrics{},
		nil,
	)
	return &framework.ScoredEndpoint{Endpoint: ep, Score: score}
}

func setPickerPrefixScore(sep *framework.ScoredEndpoint, matchBlocks, totalBlocks int) {
	info := attrprefix.NewPrefixCacheMatchInfo(matchBlocks, totalBlocks, 16)
	sep.Put(attrprefix.PrefixCacheMatchInfoKey, info)
}

func setPickerTTFT(sep *framework.ScoredEndpoint, ttft float64) {
	info := attrlatency.NewLatencyPredictionInfo(true, true, 0, 0, ttft, 0)
	sep.Put(attrlatency.LatencyPredictionInfoKey, info)
}

func pickedName(result *framework.ProfileRunResult) string {
	if len(result.TargetEndpoints) == 0 {
		return ""
	}
	return result.TargetEndpoints[0].GetMetadata().NamespacedName.Name
}

func TestAffinityWeightedPickerGating(t *testing.T) {
	tests := []struct {
		name           string
		config         AffinityPickerConfig
		endpoints      []*framework.ScoredEndpoint
		setup          func([]*framework.ScoredEndpoint)
		wantFromSticky bool // true = result must be from sticky set
		wantAnyOf      []string
	}{
		{
			name:   "disabled (tau=0) — pick from all",
			config: AffinityPickerConfig{Tau: 0},
			endpoints: []*framework.ScoredEndpoint{
				makePickerEndpoint("pod1", 0.8),
				makePickerEndpoint("pod2", 0.2),
			},
			wantAnyOf: []string{"pod1", "pod2"},
		},
		{
			name:   "no sticky candidates — pick from all",
			config: AffinityPickerConfig{Tau: 0.80, EpsilonExplore: 0},
			endpoints: []*framework.ScoredEndpoint{
				makePickerEndpoint("pod1", 0.8),
				makePickerEndpoint("pod2", 0.5),
			},
			setup: func(eps []*framework.ScoredEndpoint) {
				setPickerPrefixScore(eps[0], 5, 10) // 0.5
				setPickerPrefixScore(eps[1], 3, 10) // 0.3
			},
			wantAnyOf: []string{"pod1", "pod2"},
		},
		{
			name:   "one sticky candidate — pick it (only candidate after gate)",
			config: AffinityPickerConfig{Tau: 0.80, EpsilonExplore: 0},
			endpoints: []*framework.ScoredEndpoint{
				makePickerEndpoint("pod1", 0.5),
				makePickerEndpoint("pod2", 0.5),
			},
			setup: func(eps []*framework.ScoredEndpoint) {
				setPickerPrefixScore(eps[0], 9, 10) // 0.9 >= 0.8
				setPickerPrefixScore(eps[1], 3, 10) // 0.3
				setPickerTTFT(eps[0], 100)
				setPickerTTFT(eps[1], 90)
			},
			wantAnyOf: []string{"pod1"},
		},
		{
			name:   "sticky candidate but TTFT penalty too high — pick from all",
			config: AffinityPickerConfig{Tau: 0.80, EpsilonExplore: 0, MaxTTFTPenaltyMs: 50},
			endpoints: []*framework.ScoredEndpoint{
				makePickerEndpoint("pod1", 0.5),
				makePickerEndpoint("pod2", 0.5),
			},
			setup: func(eps []*framework.ScoredEndpoint) {
				setPickerPrefixScore(eps[0], 9, 10) // sticky
				setPickerPrefixScore(eps[1], 1, 10)
				setPickerTTFT(eps[0], 200) // slow
				setPickerTTFT(eps[1], 50)  // fast
				// penalty = 150 > 50 threshold
			},
			wantAnyOf: []string{"pod1", "pod2"},
		},
		{
			name:   "sticky candidate with acceptable TTFT penalty — pick sticky",
			config: AffinityPickerConfig{Tau: 0.80, EpsilonExplore: 0, MaxTTFTPenaltyMs: 200},
			endpoints: []*framework.ScoredEndpoint{
				makePickerEndpoint("pod1", 0.5),
				makePickerEndpoint("pod2", 0.5),
			},
			setup: func(eps []*framework.ScoredEndpoint) {
				setPickerPrefixScore(eps[0], 9, 10)
				setPickerPrefixScore(eps[1], 1, 10)
				setPickerTTFT(eps[0], 200)
				setPickerTTFT(eps[1], 50)
				// penalty = 150 < 200 threshold
			},
			wantAnyOf: []string{"pod1"},
		},
		{
			name:   "load gate disabled (maxTTFTPenaltyMs=0) — stick regardless",
			config: AffinityPickerConfig{Tau: 0.80, EpsilonExplore: 0, MaxTTFTPenaltyMs: 0},
			endpoints: []*framework.ScoredEndpoint{
				makePickerEndpoint("pod1", 0.5),
				makePickerEndpoint("pod2", 0.9),
			},
			setup: func(eps []*framework.ScoredEndpoint) {
				setPickerPrefixScore(eps[0], 9, 10) // sticky
				setPickerPrefixScore(eps[1], 1, 10)
				setPickerTTFT(eps[0], 9000) // much slower
				setPickerTTFT(eps[1], 50)
			},
			wantAnyOf: []string{"pod1"},
		},
		{
			name:   "no prefix attributes — pick from all",
			config: AffinityPickerConfig{Tau: 0.80, EpsilonExplore: 0},
			endpoints: []*framework.ScoredEndpoint{
				makePickerEndpoint("pod1", 0.5),
				makePickerEndpoint("pod2", 0.5),
			},
			wantAnyOf: []string{"pod1", "pod2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			picker := NewAffinityWeightedPicker(tt.config)
			if tt.setup != nil {
				tt.setup(tt.endpoints)
			}

			result := picker.Pick(context.Background(), framework.NewCycleState(), tt.endpoints)
			got := pickedName(result)

			allowed := make(map[string]bool)
			for _, n := range tt.wantAnyOf {
				allowed[n] = true
			}
			if !allowed[got] {
				t.Errorf("picked %q, want one of %v", got, tt.wantAnyOf)
			}
		})
	}
}

func TestAffinityWeightedPickerEpsilonExploration(t *testing.T) {
	// With epsilon=1.0, the picker should always consider all endpoints.
	config := AffinityPickerConfig{Tau: 0.80, EpsilonExplore: 1.0}
	picker := NewAffinityWeightedPicker(config)

	eps := []*framework.ScoredEndpoint{
		makePickerEndpoint("pod1", 0.5),
		makePickerEndpoint("pod2", 0.5),
	}
	setPickerPrefixScore(eps[0], 10, 10) // perfect match — sticky
	setPickerPrefixScore(eps[1], 0, 10)  // no match

	// Run many times — with epsilon=1.0, pod2 should be picked at least once.
	pickedPod2 := false
	for range 200 {
		result := picker.Pick(context.Background(), framework.NewCycleState(), eps)
		if pickedName(result) == "pod2" {
			pickedPod2 = true
			break
		}
	}
	if !pickedPod2 {
		t.Error("epsilon=1.0 should allow picking non-sticky endpoints, but pod2 was never picked in 200 trials")
	}
}

func TestAffinityWeightedPickerWeightedSelection(t *testing.T) {
	// With no affinity gate, the picker should prefer higher-scored endpoints.
	config := AffinityPickerConfig{Tau: 0} // gate disabled
	picker := NewAffinityWeightedPicker(config)

	high := makePickerEndpoint("high", 0.99)
	low := makePickerEndpoint("low", 0.01)

	highCount := 0
	for range 200 {
		result := picker.Pick(context.Background(), framework.NewCycleState(), []*framework.ScoredEndpoint{high, low})
		if pickedName(result) == "high" {
			highCount++
		}
	}

	// With 0.99 vs 0.01 score, high should be picked overwhelmingly.
	if highCount < 150 {
		t.Errorf("high-scored endpoint should be picked most of the time, got %d/200", highCount)
	}
}
