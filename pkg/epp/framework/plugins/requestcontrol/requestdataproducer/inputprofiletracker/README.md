# Input Profile Tracker Plugin (`input-profile-tracker`)

Observes incoming request characteristics and produces representative traffic statistics
as endpoint attributes for downstream plugins.

## Interface

PrepareDataPlugin

## What It Does

On every request, the tracker:

1. **Counts input tokens** using word count (`len(strings.Fields(promptText))`) — the same heuristic the training server uses
2. **Reads prefix cache scores** from endpoint attributes (if available from `prefix-cache-scorer`)
3. **Computes effective input**: `effectiveInput = inputTokens * (1 - prefixCacheScore)`
4. **Records the full observation** (inputTokens, prefixCacheScore, effectiveInput) in a ring buffer
5. **Produces `InputProfileInfo`** on every endpoint with the observation at the configured
   percentile of effective input

Effective input captures both request size and cache savings in a single number. A 1000-token
request with 0.9 cache hit (effective = 100) is very different from a 100-token request with
0 cache (effective = 100) — the tracker preserves both original values so the sidecar model
gets the features it was trained on.

## Produced Attribute

| Key | Type | Fields |
|-----|------|--------|
| `InputProfileInfoKey` | `InputProfileInfo` | `InputTokens() int`, `PrefixCacheScore() float64`, `EffectiveInputTokens() int` |

The attribute is set on **every** endpoint during PrepareRequestData. All endpoints get the
same pool-level values since the profile reflects aggregate traffic.

## Consumed Attribute

| Key | Type | Source |
|-----|------|--------|
| `PrefixCacheMatchInfoKey` | `PrefixCacheMatchInfo` | `prefix-cache-scorer` |

Optional — if prefix cache data isn't available, prefix cache score defaults to 0.

## Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| `windowDuration` | 5m | How far back observations are kept. |
| `maxSamples` | 10000 | Maximum observations stored (ring buffer). Oldest evicted when full. |
| `percentile` | 90 | Percentile (0-100) of effective input for selecting the representative observation. |

## Direct Interface

The tracker also exposes `InputProfileProvider` for plugins that need profile data outside
the scheduling path (e.g., background probe loops):

```go
type InputProfileProvider interface {
    ProbeProfile(fallbackTokens int, fallbackCache float64) (inputTokens int, prefixCacheScore float64)
}
```

`ProbeProfile` returns the `(inputTokens, prefixCacheScore)` pair from the observation at
the configured percentile of effective input. Both original values are returned so the
prediction sidecar receives the same features it was trained on.

The `latency-detector` plugin discovers this interface via the plugin Handle at startup.
If the tracker isn't configured, the detector falls back to static config values.

## Example ConfigMap

```yaml
plugins:
- type: prefix-cache-scorer
- type: input-profile-tracker
  parameters:
    windowDuration: "5m"
    maxSamples: 10000
    percentile: 90
- type: latency-detector
  parameters:
    e2eSLOMs: 200
    probeInputTokenLength: 512    # fallback until tracker has data
schedulingProfiles:
- name: default
  plugins:
  - pluginRef: prefix-cache-scorer
  - pluginRef: input-profile-tracker
  - pluginRef: latency-detector
```

## Files

| File | Purpose |
|------|---------|
| `tracker.go` | Plugin implementation: observation, ring buffer, attribute production |
| `tracker_test.go` | Unit tests for percentile, windowing, ring buffer, attribute output |
| `attribute/inputprofile/data_types.go` | `InputProfileInfo` attribute type |
