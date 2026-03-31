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

// Package idlepreference provides a filter that narrows candidates to idle
// endpoints (zero dispatched requests) when any exist. If no idle endpoints
// exist, all endpoints are kept.
package idlepreference

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
	PluginType = "idle-endpoint-filter"
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

func (p *Plugin) Filter(ctx context.Context, _ *framework.CycleState, _ *framework.LLMRequest, endpoints []framework.Endpoint) []framework.Endpoint {
	logger := log.FromContext(ctx)

	if len(endpoints) <= 1 {
		return endpoints
	}

	var idle []framework.Endpoint
	for _, ep := range endpoints {
		if raw, ok := ep.Get(attrlatency.LatencyPredictionInfoKey); ok {
			info := raw.(*attrlatency.LatencyPredictionInfo)
			if info.DispatchedRequestCount() == 0 {
				idle = append(idle, ep)
			}
		}
	}

	if len(idle) == 0 {
		return endpoints
	}

	logger.V(logutil.DEBUG).Info("IdlePreferenceFilter: narrowed to idle endpoints",
		"idle", len(idle), "total", len(endpoints))
	return idle
}

func (p *Plugin) Consumes() map[string]any {
	return map[string]any{
		attrlatency.LatencyPredictionInfoKey: attrlatency.LatencyPredictionInfo{},
	}
}
