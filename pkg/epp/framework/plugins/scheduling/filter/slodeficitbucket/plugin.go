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

// Package slodeficitbucket provides a filter that groups negative-headroom
// endpoints by which SLOs they violate and returns only the best (least
// severe) non-empty bucket.
package slodeficitbucket

import (
	"context"
	"encoding/json"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/observability/logging"
	fwkplugin "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrlatency "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/latency"
)

const (
	PluginType = "slo-deficit-bucket-filter"
)

var _ framework.Filter = &Plugin{}

type Plugin struct {
	typedName fwkplugin.TypedName
}

func Factory(name string, _ json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	return &Plugin{
		typedName: fwkplugin.TypedName{Type: PluginType, Name: name},
	}, nil
}

func (p *Plugin) TypedName() fwkplugin.TypedName {
	return p.typedName
}

// Filter groups endpoints by which SLOs they violate and returns only the
// best (least severe) non-empty bucket.
//
// Priority order (most preferred first):
//  1. Only TPOT negative (TTFT is met)
//  2. Only TTFT negative (TTFT impacts perceived responsiveness most)
//  3. Both TTFT and TPOT negative (violates both SLOs)
//
// If all endpoints have positive headroom (no violations), all are returned
// unchanged. This filter is designed to run after the slo-headroom-tier-filter
// has selected the negative tier.
func (p *Plugin) Filter(ctx context.Context, _ *framework.CycleState, _ *framework.LLMRequest, endpoints []framework.Endpoint) []framework.Endpoint {
	logger := log.FromContext(ctx)

	if len(endpoints) <= 1 {
		return endpoints
	}

	var negTPOTonly, negTTFTonly, bothNeg, positive []framework.Endpoint

	for _, ep := range endpoints {
		raw, ok := ep.Get(attrlatency.LatencyPredictionInfoKey)
		if !ok {
			bothNeg = append(bothNeg, ep) // no prediction, treat as worst
			continue
		}
		info := raw.(*attrlatency.LatencyPredictionInfo)
		ttftNeg := info.TTFTHeadroom() < 0
		tpotNeg := info.TPOTHeadroom() < 0
		switch {
		case ttftNeg && tpotNeg:
			bothNeg = append(bothNeg, ep)
		case ttftNeg:
			negTTFTonly = append(negTTFTonly, ep)
		case tpotNeg:
			negTPOTonly = append(negTPOTonly, ep)
		default:
			positive = append(positive, ep)
		}
	}

	// All positive or no violations, return all.
	if len(positive) == len(endpoints) || (len(negTPOTonly) == 0 && len(negTTFTonly) == 0 && len(bothNeg) == 0) {
		return endpoints
	}

	if len(negTPOTonly) > 0 {
		logger.V(logutil.DEBUG).Info("SLODeficitBucketFilter: tpot-only bucket",
			"count", len(negTPOTonly), "total", len(endpoints))
		return negTPOTonly
	}
	if len(negTTFTonly) > 0 {
		logger.V(logutil.DEBUG).Info("SLODeficitBucketFilter: ttft-only bucket",
			"count", len(negTTFTonly), "total", len(endpoints))
		return negTTFTonly
	}
	logger.V(logutil.DEBUG).Info("SLODeficitBucketFilter: both-neg bucket",
		"count", len(bothNeg), "total", len(endpoints))
	return bothNeg
}

func (p *Plugin) Consumes() map[string]any {
	return map[string]any{
		attrlatency.LatencyPredictionInfoKey: attrlatency.LatencyPredictionInfo{},
	}
}
