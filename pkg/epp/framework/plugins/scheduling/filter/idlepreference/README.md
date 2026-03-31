# Idle Preference Filter (`idle-endpoint-filter`)

Filter that narrows candidates to idle endpoints (zero dispatched requests) when any
exist. If no idle endpoints exist, all endpoints are kept.

Uses the EPP's internal dispatched request count (from `LatencyPredictionInfo`), not the
model server's `RunningRequestsSize` metric, for more accurate tracking of in-flight
requests dispatched by this EPP instance.

## Behavior

- If any endpoint has `DispatchedRequestCount == 0`, keep only those
- Otherwise keep all endpoints

## Config

None.

## Dependencies

- Reads `LatencyPredictionInfo` from endpoint attributes (from `predicted-latency-producer`)
