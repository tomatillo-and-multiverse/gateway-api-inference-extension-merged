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

package idlepreference

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"

	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	framework "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/scheduling"
	attrlatency "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/plugins/datalayer/attribute/latency"
)

func makeEndpoint(name string, dispatchedCount int) framework.Endpoint {
	meta := &fwkdl.EndpointMetadata{
		NamespacedName: types.NamespacedName{Name: name, Namespace: "default"},
	}
	ep := framework.NewEndpoint(meta, &fwkdl.Metrics{}, fwkdl.NewAttributes())
	ep.Put(attrlatency.LatencyPredictionInfoKey, attrlatency.NewLatencyPredictionInfoWithDispatch(
		true, true, 0, 0, 100, 10, dispatchedCount))
	return ep
}

func TestFilter_SingleEndpoint(t *testing.T) {
	p := &Plugin{}
	endpoints := []framework.Endpoint{makeEndpoint("a", 5)}
	result := p.Filter(context.Background(), nil, nil, endpoints)
	assert.Equal(t, 1, len(result))
}

func TestFilter_AllBusy(t *testing.T) {
	p := &Plugin{}
	endpoints := []framework.Endpoint{
		makeEndpoint("a", 5),
		makeEndpoint("b", 3),
		makeEndpoint("c", 10),
	}
	result := p.Filter(context.Background(), nil, nil, endpoints)
	assert.Equal(t, 3, len(result), "no idle endpoints, keep all")
}

func TestFilter_AllIdle(t *testing.T) {
	p := &Plugin{}
	endpoints := []framework.Endpoint{
		makeEndpoint("a", 0),
		makeEndpoint("b", 0),
	}
	result := p.Filter(context.Background(), nil, nil, endpoints)
	assert.Equal(t, 2, len(result), "all idle, keep all")
}

func TestFilter_MixedIdleAndBusy(t *testing.T) {
	p := &Plugin{}
	endpoints := []framework.Endpoint{
		makeEndpoint("idle1", 0),
		makeEndpoint("busy1", 5),
		makeEndpoint("idle2", 0),
		makeEndpoint("busy2", 3),
	}
	result := p.Filter(context.Background(), nil, nil, endpoints)
	assert.Equal(t, 2, len(result), "should keep only idle")
	assert.Equal(t, "idle1", result[0].GetMetadata().NamespacedName.Name)
	assert.Equal(t, "idle2", result[1].GetMetadata().NamespacedName.Name)
}

func TestFilter_NoPredictions(t *testing.T) {
	p := &Plugin{}
	meta := &fwkdl.EndpointMetadata{
		NamespacedName: types.NamespacedName{Name: "nopred", Namespace: "default"},
	}
	ep := framework.NewEndpoint(meta, &fwkdl.Metrics{}, fwkdl.NewAttributes())
	endpoints := []framework.Endpoint{ep, makeEndpoint("busy", 5)}
	result := p.Filter(context.Background(), nil, nil, endpoints)
	assert.Equal(t, 2, len(result), "no predictions on some, keep all")
}

func TestFactory(t *testing.T) {
	plugin, err := Factory("test", nil, nil)
	assert.NoError(t, err)
	assert.NotNil(t, plugin)
	assert.Equal(t, PluginType, plugin.TypedName().Type)
}
