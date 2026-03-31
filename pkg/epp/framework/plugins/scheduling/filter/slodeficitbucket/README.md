# SLO Deficit Bucket Filter (`slo-deficit-bucket-filter`)

Filter that groups negative-headroom endpoints by which SLOs they violate and
returns only the best (least severe) non-empty bucket.

Designed to run after `slo-headroom-tier-filter` selects the negative tier and
after `prefix-cache-affinity-filter` narrows to sticky endpoints.

## Behavior

Endpoints are grouped into three buckets by deficit type. The first non-empty
bucket is returned (most preferred first):
1. Only TPOT negative (TTFT is met - most preferred)
2. Only TTFT negative (TTFT impacts perceived responsiveness most)
3. Both TTFT and TPOT negative (violates both SLOs - least preferred)

If all endpoints have positive headroom (no violations), all are returned
unchanged.

## Config

None.

## Inputs

- `LatencyPredictionInfo` endpoint attribute:
  - `TTFTHeadroom` / `TPOTHeadroom` for deficit classification
