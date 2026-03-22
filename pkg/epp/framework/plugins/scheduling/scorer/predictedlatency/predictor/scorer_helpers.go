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
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	errcommon "sigs.k8s.io/gateway-api-inference-extension/pkg/common/error"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/common/observability/logging"
	schedulingtypes "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
)

func (s *PredictedLatency) parseSLOHeaders(ctx context.Context, request *schedulingtypes.LLMRequest, predictedLatencyCtx *predictedLatencyCtx) {
	logger := log.FromContext(ctx)
	var err error

	// Get Request SLOs from request header
	predictedLatencyCtx.ttftSLO, err = parseFloatHeader(*request, TTFTSLOHeaderKey)
	if err != nil {
		logger.V(logutil.DEBUG).Error(errcommon.Error{Code: errcommon.BadRequest, Msg: fmt.Sprintf("%v must be a float: %v", TTFTSLOHeaderKey, err)}, "PredictedLatency: Error parsing TTFT SLO from header")
	}

	predictedLatencyCtx.avgTPOTSLO, err = parseFloatHeader(*request, TPOTSLOHeaderKey)
	if err != nil {
		logger.V(logutil.DEBUG).Error(errcommon.Error{Code: errcommon.BadRequest, Msg: fmt.Sprintf("%v must be a float: %v", TPOTSLOHeaderKey, err)}, "PredictedLatency: Error parsing TPOT SLO from header")
	}
}

func (s *PredictedLatency) getEndpointMinTPOTSLO(endpoint schedulingtypes.Endpoint) float64 {
	endpointName := endpoint.GetMetadata().NamespacedName
	if runningReqs := s.getRunningRequestList(endpointName); runningReqs != nil && runningReqs.GetSize() > 0 {
		if min := runningReqs.Peek(); min != nil {
			return min.tpot
		}
	}
	return 0
}

func (s *PredictedLatency) getEndpointRunningRequestCount(endpoint schedulingtypes.Endpoint) int {
	endpointName := endpoint.GetMetadata().NamespacedName
	if runningReqs := s.getRunningRequestList(endpointName); runningReqs != nil {
		return runningReqs.GetSize()
	}
	return 0
}

func (s *PredictedLatency) getRunningRequestList(endpointName types.NamespacedName) *requestPriorityQueue {
	if value, ok := s.runningRequestLists.Load(endpointName); ok {
		return value.(*requestPriorityQueue)
	}
	return nil
}

func (s *PredictedLatency) removeRequestFromEndpoint(endpointName types.NamespacedName, requestID string) {
	if queue := s.getRunningRequestList(endpointName); queue != nil {
		queue.Remove(requestID)
	}
}

func (s *PredictedLatency) removeRequestFromQueue(requestID string, ctx *predictedLatencyCtx) {
	if ctx == nil || ctx.targetMetadata == nil {
		return
	}
	endpointName := types.NamespacedName{
		Name:      ctx.targetMetadata.NamespacedName.Name,
		Namespace: ctx.targetMetadata.NamespacedName.Namespace,
	}
	s.removeRequestFromEndpoint(endpointName, requestID)
}
