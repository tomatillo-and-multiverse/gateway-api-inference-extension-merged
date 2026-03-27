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

package requestinput

import (
	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
)

const (
	// RequestInputInfoKey is the endpoint attribute key for request input data.
	RequestInputInfoKey = "RequestInputInfoKey"
)

// RequestInputInfo contains the input token count for the current request.
//
// IMPORTANT: InputTokenCount uses word count (len(strings.Fields(promptText))) to match
// the training server's heuristic. Do NOT change this to actual token IDs or byte-based
// estimates unless the training server and all trained models are updated to use the same
// method. Changing this without retraining will silently degrade prediction accuracy.
type RequestInputInfo struct {
	inputTokenCount int
}

// NewRequestInputInfo creates a new RequestInputInfo.
func NewRequestInputInfo(inputTokenCount int) *RequestInputInfo {
	return &RequestInputInfo{inputTokenCount: inputTokenCount}
}

// InputTokenCount returns the input token count (word count) for the request.
func (r *RequestInputInfo) InputTokenCount() int { return r.inputTokenCount }

// Clone implements fwkdl.Cloneable.
func (r *RequestInputInfo) Clone() fwkdl.Cloneable {
	if r == nil {
		return nil
	}
	return &RequestInputInfo{inputTokenCount: r.inputTokenCount}
}
