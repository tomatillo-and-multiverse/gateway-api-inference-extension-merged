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

const (
	// LatencyDataProviderPluginType is the plugin type for the latency predictor.
	// It trains XGBoost models via the sidecar and generates predictions for scoring.
	LatencyDataProviderPluginType = "latency-predictor"

	// TTFTSLOHeaderKey is the header key for the TTFT SLO.
	TTFTSLOHeaderKey = "x-slo-ttft-ms"
	// TPOTSLOHeaderKey is the header key for the TPOT SLO.
	TPOTSLOHeaderKey = "x-slo-tpot-ms"
)
