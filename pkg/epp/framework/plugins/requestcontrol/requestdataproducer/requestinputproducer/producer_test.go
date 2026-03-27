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

package requestinputproducer

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrreqinput "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/requestinput"
)

func makeEndpoint(name string) framework.Endpoint {
	return framework.NewEndpoint(
		&fwkdl.EndpointMetadata{NamespacedName: k8stypes.NamespacedName{Name: name}},
		&fwkdl.Metrics{UpdateTime: time.Now()},
		nil,
	)
}

func TestProducer_PrepareRequestData(t *testing.T) {
	producer := &Producer{}

	request := &framework.LLMRequest{
		Body: &framework.LLMRequestBody{
			Completions: &framework.CompletionsRequest{
				Prompt: "the quick brown fox jumps over the lazy dog",
			},
		},
	}

	ep1 := makeEndpoint("pod1")
	ep2 := makeEndpoint("pod2")
	endpoints := []framework.Endpoint{ep1, ep2}

	err := producer.PrepareRequestData(context.Background(), request, endpoints)
	require.NoError(t, err)

	// Both endpoints should have the same attribute with word count = 9.
	for _, ep := range endpoints {
		raw, ok := ep.Get(attrreqinput.RequestInputInfoKey)
		require.True(t, ok)
		info := raw.(*attrreqinput.RequestInputInfo)
		require.Equal(t, 9, info.InputTokenCount())
	}
}

func TestProducer_PrepareRequestData_NoBody(t *testing.T) {
	producer := &Producer{}

	err := producer.PrepareRequestData(context.Background(), &framework.LLMRequest{}, []framework.Endpoint{makeEndpoint("pod1")})
	require.NoError(t, err)

	// No body → no attribute set.
	ep := makeEndpoint("pod1")
	_, ok := ep.Get(attrreqinput.RequestInputInfoKey)
	require.False(t, ok)
}

func TestProducer_PrepareRequestData_EmptyPrompt(t *testing.T) {
	producer := &Producer{}

	request := &framework.LLMRequest{
		Body: &framework.LLMRequestBody{
			Completions: &framework.CompletionsRequest{Prompt: ""},
		},
	}

	ep := makeEndpoint("pod1")
	err := producer.PrepareRequestData(context.Background(), request, []framework.Endpoint{ep})
	require.NoError(t, err)

	// Empty prompt → word count = 0 → no attribute set.
	_, ok := ep.Get(attrreqinput.RequestInputInfoKey)
	require.False(t, ok)
}

func TestCountInputTokens_WordCount(t *testing.T) {
	// Must use word count to match training server.
	request := &framework.LLMRequest{
		Body: &framework.LLMRequestBody{
			Completions: &framework.CompletionsRequest{
				Prompt: "one two three",
			},
		},
	}
	require.Equal(t, 3, countInputTokens(request))
}

func TestProducer_TypedName(t *testing.T) {
	p := &Producer{}
	tn := p.TypedName()
	require.Equal(t, RequestInputProducerType, tn.Type)
}
