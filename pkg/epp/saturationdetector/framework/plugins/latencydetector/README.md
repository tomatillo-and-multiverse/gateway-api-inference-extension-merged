# Latency Detector Plugin (`latency-detector`)

Detects endpoint saturation using ML-predicted latency from the latency predictor sidecar.

## Interfaces

- **SaturationDetector** (flow control) — background probe returns `predicted_latency / SLO`
- **Filter** (scheduling) — removes endpoints whose predicted latency exceeds SLO

## How It Works

### Background Probing (Saturation Signal)

A background goroutine periodically probes each endpoint by sending a synthetic prediction
request to the latency predictor sidecar. The request uses the endpoint's live metrics
(queue depth, KV cache utilization) and a representative input token count.

If the `input-profile-tracker` plugin is configured, the probe uses the **(inputTokens,
prefixCacheScore) pair from the observation at the p90 of effective input**
(`inputTokens * (1 - prefixCacheScore)`). This represents a heavy-but-not-extreme request
with its actual cache savings, preserving both features for the sidecar model. Without the
tracker, the probe falls back to static config values (`probeInputTokenLength`,
`probePrefixCacheScore`).

Non-streaming:

```
EndpointSaturation = PredictedE2ELatency / e2eSLOMs
```

Streaming:

```
EndpointSaturation = Max(PredictedTTFT / ttftSLOMs, PredictedTPOT / tpotSLOMs)
```

Pool saturation is the average across all endpoints. The flow controller consumes this
signal: `>= 1.0` triggers backpressure, `< 1.0` allows dispatch.

### Per-Request Filtering (Scheduling)

During scheduling, the Filter reads `LatencyPredictionInfo` from endpoint attributes
(populated by the `latency-predictor-producer` plugin in PrepareRequestData). Endpoints
whose headroom is negative (predicted latency exceeds SLO) are filtered out. An optional
`headroom` burst allowance relaxes this threshold.

If all endpoints would be filtered, the plugin fails open and returns all endpoints.

## Non-Streaming vs Streaming

The mode is determined by which SLO field is set:

| Mode | SLO Field |
|------|-----------|
| Non-streaming | `e2eSLOMs` |
| Streaming | `ttftSLOMs` (+ optional `tpotSLOMs`) |

Exactly one of `e2eSLOMs` or `ttftSLOMs` must be set. This determines how the predictor's
output is interpreted. It must be consistent with the `latency-predictor-producer` plugin's
`streamingMode` setting so that training data and predictions are aligned.

## Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| `e2eSLOMs` | 200 | E2E latency SLO (ms). Set for non-streaming workloads. Mutually exclusive with `ttftSLOMs`. |
| `ttftSLOMs` | - | TTFT SLO (ms). Set for streaming workloads. Mutually exclusive with `e2eSLOMs`. |
| `tpotSLOMs` | - | TPOT SLO (ms). Only used with `ttftSLOMs`. |
| `probeInputTokenLength` | 512 | Fallback input token count for probes. Overridden by `input-profile-tracker` when available. |
| `probePrefixCacheScore` | 0.0 | Fallback prefix cache hit ratio for probes. Overridden by `input-profile-tracker` when available. |
| `probeInterval` | 10s | How often the background goroutine probes all endpoints. |
| `headroom` | 0.0 | Burst allowance fraction for Filter. E.g., 0.2 = allow 20% over SLO before filtering. |

### Validation Rules

- Exactly one of `e2eSLOMs` or `ttftSLOMs` must be > 0
- `tpotSLOMs` requires `ttftSLOMs` (streaming mode)
- Setting both `e2eSLOMs` and `ttftSLOMs` is an error

## Metrics

| Metric | Labels | Description |
|--------|--------|-------------|
| `inference_extension_latency_detector_endpoint_saturation` | `endpoint` | Per-endpoint predicted saturation from probe |
| `inference_extension_latency_detector_pool_saturation` | - | Aggregate saturation (average across endpoints) |

The existing `inference_extension_flow_control_pool_saturation` metric is also emitted
automatically by the flow controller when it calls `Saturation()`.

## Input Profile Tracker (`input-profile-tracker`)

An optional companion `PrepareDataPlugin` that observes every incoming request and tracks
effective input: `inputTokens * (1 - prefixCacheScore)`. Observations are ranked by
effective input and the observation at the configured percentile is selected, preserving
both original (inputTokens, prefixCacheScore) values for probing.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `windowDuration` | 5m | How far back observations are kept. |
| `maxSamples` | 10000 | Maximum observations stored (ring buffer). |
| `percentile` | 90 | Percentile of effective input for selecting the representative observation. |

Token estimation uses word count (`len(strings.Fields(promptText))`) to match the training
server's heuristic.

See the [input-profile-tracker README](../../framework/plugins/requestcontrol/requestdataproducer/inputprofiletracker/README.md) for full details.

## Dependencies

- Requires the **latency predictor sidecar** (training + prediction servers) to be deployed
  as containers in the EPP pod. The plugin starts its own sidecar client using environment
  variables (`PREDICTION_SERVER_URL`, `TRAINING_SERVER_URL`, etc.).
- For the Filter to work, the `latency-predictor-producer` plugin must run first in
  PrepareRequestData to populate `LatencyPredictionInfo` endpoint attributes.
- The `input-profile-tracker` is optional. Without it, the detector uses static
  `probeInputTokenLength` and `probePrefixCacheScore` config values.

## Example ConfigMap

### Non-streaming (E2E latency)

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: epp-config
data:
  default-plugins.yaml: |
    apiVersion: inference.networking.x-k8s.io/v1alpha1
    kind: EndpointPickerConfig
    plugins:
    - type: metrics-data-source
      parameters:
        scheme: "http"
        path: "/metrics"
        insecureSkipVerify: true
    - type: core-metrics-extractor
    - type: prefix-cache-scorer
    - type: latency-predictor-producer
    - type: latency-scorer
    - type: latency-admission
    - type: input-profile-tracker
      parameters:
        windowDuration: "5m"
        percentile: 90
    - type: latency-detector
      parameters:
        e2eSLOMs: 1000
        probeInterval: "10s"
        headroom: 0.1
    - type: weighted-random-picker
    schedulingProfiles:
    - name: default
      plugins:
      - pluginRef: input-profile-tracker
      - pluginRef: latency-predictor-producer
      - pluginRef: latency-scorer
        weight: 1
      - pluginRef: latency-admission
      - pluginRef: latency-detector
      - pluginRef: weighted-random-picker
    featureGates:
    - prepareDataPlugins
```

### Streaming (TTFT + TPOT)

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: epp-config
data:
  default-plugins.yaml: |
    apiVersion: inference.networking.x-k8s.io/v1alpha1
    kind: EndpointPickerConfig
    plugins:
    - type: metrics-data-source
      parameters:
        scheme: "http"
        path: "/metrics"
        insecureSkipVerify: true
    - type: core-metrics-extractor
    - type: prefix-cache-scorer
    - type: latency-predictor-producer
      parameters:
        streamingMode: true
    - type: latency-scorer
    - type: latency-admission
    - type: input-profile-tracker
      parameters:
        windowDuration: "5m"
        percentile: 90
    - type: latency-detector
      parameters:
        ttftSLOMs: 200
        tpotSLOMs: 50
        probeInputTokenLength: 1024
        probeInterval: "10s"
        headroom: 0.2
    - type: weighted-random-picker
    schedulingProfiles:
    - name: default
      plugins:
      - pluginRef: input-profile-tracker
      - pluginRef: latency-predictor-producer
      - pluginRef: latency-scorer
        weight: 1
      - pluginRef: latency-admission
      - pluginRef: latency-detector
      - pluginRef: weighted-random-picker
    featureGates:
    - prepareDataPlugins
```

## Files

| File | Purpose |
|------|---------|
| `config.go` | Config struct, defaults, validation |
| `detector.go` | Saturation(), Filter(), background probe loop, metrics emission |
| `detector_test.go` | Unit tests for saturation, filtering, probing, validation |
