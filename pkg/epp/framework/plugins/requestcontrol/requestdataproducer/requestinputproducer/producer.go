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

// Package requestinputproducer implements a PrepareDataPlugin that computes the input token
// count for each request and produces it as a RequestInputInfo endpoint attribute.
//
// IMPORTANT: The token count uses word count (len(strings.Fields(promptText))) to match the
// latency predictor training server's heuristic. Do NOT change this to actual token IDs or
// byte-based estimates unless the training server and all trained models are updated. Changing
// this without retraining will silently degrade prediction accuracy.
//
// This plugin is the single source of truth for input token counting. Downstream consumers
// (input-profile-tracker, and eventually latency-predictor-producer) should read
// RequestInputInfoKey from endpoints instead of computing their own token counts.
package requestinputproducer

import (
	"context"
	"encoding/json"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/observability/logging"
	fwkplugin "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/requestcontrol"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrreqinput "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/requestinput"
)

const (
	// RequestInputProducerType is the unique identifier for this plugin.
	RequestInputProducerType = "request-input-producer"
)

var _ requestcontrol.PrepareDataPlugin = &Producer{}

// Producer computes input token count and stores it as an endpoint attribute.
type Producer struct{}

// ProducerFactory creates a new request input producer plugin.
func ProducerFactory(_ string, _ json.RawMessage, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	return &Producer{}, nil
}

// TypedName returns the type and name tuple of this plugin instance.
func (p *Producer) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{
		Type: RequestInputProducerType,
		Name: RequestInputProducerType,
	}
}

// PrepareRequestData computes the input token count and stores it on every endpoint.
func (p *Producer) PrepareRequestData(ctx context.Context, request *framework.LLMRequest, endpoints []framework.Endpoint) error {
	logger := log.FromContext(ctx)

	tokenCount := countInputTokens(request)
	if tokenCount <= 0 {
		return nil
	}

	info := attrreqinput.NewRequestInputInfo(tokenCount)
	for _, ep := range endpoints {
		ep.Put(attrreqinput.RequestInputInfoKey, info)
	}

	logger.V(logutil.TRACE).Info("Request input produced", "inputTokenCount", tokenCount)
	return nil
}

// Produces declares that this plugin produces RequestInputInfo on endpoints.
func (p *Producer) Produces() map[string]any {
	return map[string]any{
		attrreqinput.RequestInputInfoKey: attrreqinput.RequestInputInfo{},
	}
}

// Consumes returns nil — this plugin reads directly from the LLMRequest.
func (p *Producer) Consumes() map[string]any { return nil }

// countInputTokens computes the input token count using word count.
// This MUST match the training server's heuristic: len(strings.Fields(promptText)).
func countInputTokens(request *framework.LLMRequest) int {
	if request == nil || request.Body == nil {
		return 0
	}
	return len(strings.Fields(request.Body.PromptText()))
}
