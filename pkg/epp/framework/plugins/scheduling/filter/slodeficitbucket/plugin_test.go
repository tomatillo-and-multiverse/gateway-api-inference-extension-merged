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

package slodeficitbucket

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"

	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrlatency "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/latency"
)

func makeEndpoint(name string, ttftHeadroom, tpotHeadroom float64, hasPrediction bool) framework.Endpoint {
	meta := &fwkdl.EndpointMetadata{
		NamespacedName: types.NamespacedName{Name: name, Namespace: "default"},
	}
	ep := framework.NewEndpoint(meta, &fwkdl.Metrics{}, fwkdl.NewAttributes())
	if hasPrediction {
		ep.Put(attrlatency.LatencyPredictionInfoKey, attrlatency.NewLatencyPredictionInfo(
			ttftHeadroom >= 0, tpotHeadroom >= 0, ttftHeadroom, tpotHeadroom, 100, 10))
	}
	return ep
}

func TestFilter_SingleEndpoint(t *testing.T) {
	p := &Plugin{}
	endpoints := []framework.Endpoint{makeEndpoint("a", -100, -50, true)}
	result := p.Filter(context.Background(), nil, nil, endpoints)
	assert.Equal(t, 1, len(result))
}

func TestFilter_AllPositive(t *testing.T) {
	p := &Plugin{}
	endpoints := []framework.Endpoint{
		makeEndpoint("a", 100, 50, true),
		makeEndpoint("b", 200, 80, true),
	}
	result := p.Filter(context.Background(), nil, nil, endpoints)
	assert.Equal(t, 2, len(result), "all positive, keep all")
}

func TestFilter_TPOTOnlyPreferred(t *testing.T) {
	p := &Plugin{}
	endpoints := []framework.Endpoint{
		makeEndpoint("both-neg", -100, -50, true),
		makeEndpoint("ttft-only", -100, 50, true),
		makeEndpoint("tpot-only", 100, -50, true),
	}
	result := p.Filter(context.Background(), nil, nil, endpoints)
	assert.Equal(t, 1, len(result), "should return only tpot-only bucket")
	assert.Equal(t, "tpot-only", result[0].GetMetadata().NamespacedName.Name)
}

func TestFilter_TTFTOnlyWhenNoTPOTOnly(t *testing.T) {
	p := &Plugin{}
	endpoints := []framework.Endpoint{
		makeEndpoint("both-neg", -100, -50, true),
		makeEndpoint("ttft-only", -100, 50, true),
	}
	result := p.Filter(context.Background(), nil, nil, endpoints)
	assert.Equal(t, 1, len(result), "should return only ttft-only bucket")
	assert.Equal(t, "ttft-only", result[0].GetMetadata().NamespacedName.Name)
}

func TestFilter_BothNegWhenOnly(t *testing.T) {
	p := &Plugin{}
	endpoints := []framework.Endpoint{
		makeEndpoint("both1", -100, -50, true),
		makeEndpoint("both2", -200, -80, true),
	}
	result := p.Filter(context.Background(), nil, nil, endpoints)
	assert.Equal(t, 2, len(result), "should return all both-neg")
}

func TestFilter_NoPrediction(t *testing.T) {
	p := &Plugin{}
	endpoints := []framework.Endpoint{
		makeEndpoint("nopred", 0, 0, false),
		makeEndpoint("both-neg", -100, -50, true),
	}
	result := p.Filter(context.Background(), nil, nil, endpoints)
	assert.Equal(t, 2, len(result), "both in both-neg bucket")
}

func TestFactory(t *testing.T) {
	plugin, err := Factory("test", nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, plugin)
	assert.Equal(t, PluginType, plugin.TypedName().Type)
}
