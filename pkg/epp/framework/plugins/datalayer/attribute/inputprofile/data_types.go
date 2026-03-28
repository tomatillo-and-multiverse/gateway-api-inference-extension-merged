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

package inputprofile

import (
	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
)

const (
	// InputProfileInfoKey is the endpoint attribute key for input profile data.
	InputProfileInfoKey = "InputProfileInfoKey"
)

// InputProfileInfo contains representative traffic characteristics observed from real requests.
// Produced by the input-profile-tracker plugin. The observation at the configured percentile
// of effective input is selected, preserving both original values so the sidecar model
// receives the same features it was trained on.
type InputProfileInfo struct {
	// inputWords is the input word count from the representative request.
	inputWords int
	// prefixCacheScore is the prefix cache score from the representative request.
	prefixCacheScore float64
	// effectiveInputWords is inputWords * (1 - prefixCacheScore), used for
	// percentile ranking.
	effectiveInputWords int
}

// NewInputProfileInfo creates a new InputProfileInfo.
func NewInputProfileInfo(inputWords int, prefixCacheScore float64, effectiveInputWords int) *InputProfileInfo {
	return &InputProfileInfo{
		inputWords:          inputWords,
		prefixCacheScore:    prefixCacheScore,
		effectiveInputWords: effectiveInputWords,
	}
}

// InputWords returns the input word count from the representative request.
func (i *InputProfileInfo) InputWords() int { return i.inputWords }

// PrefixCacheScore returns the prefix cache score from the representative request.
func (i *InputProfileInfo) PrefixCacheScore() float64 { return i.prefixCacheScore }

// EffectiveInputWords returns the effective input (inputWords * (1 - prefixCacheScore)).
func (i *InputProfileInfo) EffectiveInputWords() int { return i.effectiveInputWords }

// Clone implements fwkdl.Cloneable.
func (i *InputProfileInfo) Clone() fwkdl.Cloneable {
	if i == nil {
		return nil
	}
	return &InputProfileInfo{
		inputWords:          i.inputWords,
		prefixCacheScore:    i.prefixCacheScore,
		effectiveInputWords: i.effectiveInputWords,
	}
}
