# Grafana Dashboard Plan for Trama Saga Orchestrator

This document describes a production-grade Grafana dashboard using the current Prometheus metrics exposed by Trama.

## Objectives
- Give teams a fast, accurate view of saga execution health.
- Enable drill-down by saga definition (`saga_name`, `saga_version`).
- Surface failures, latency regressions, retries, and queue pressure.
- Support actionable alerting for SRE and feature teams.

## Available Metrics (current)
Custom metrics exposed by Trama:
- `saga_enqueue_total` with labels: `saga_name`, `saga_version`, `phase`
- `saga_dequeue_total` with labels: `saga_name`, `saga_version`, `phase`
- `saga_processed_total` with labels: `saga_name`, `saga_version`, `phase`, `outcome`
- `saga_failed_total` with labels: `saga_name`, `saga_version`, `phase`, `reason`
- `saga_retried_total` with labels: `saga_name`, `saga_version`, `phase`
- `saga_rate_limited_total` with labels: `saga_name`, `saga_version`, `phase`
- `saga_inmemory_queue_size`
- `saga_duration_seconds_{bucket,sum,count}` with labels: `saga_name`, `saga_version`, `final_status`
- `saga_step_duration_success_seconds_{bucket,sum,count}` with labels: `saga_name`, `saga_version`, `step_name`

## Recommended Grafana Variables
Create these template variables to filter/group panels.

1. `saga_name`
- Query: `label_values(saga_duration_seconds_count, saga_name)`
- Include all option: `.*`
- Multi-value: enabled

2. `saga_version`
- Query: `label_values(saga_duration_seconds_count{saga_name=~"$saga_name"}, saga_version)`
- Include all option: `.*`
- Multi-value: enabled

3. `final_status`
- Query: `label_values(saga_duration_seconds_count, final_status)`
- Default: `SUCCEEDED|FAILED|CORRUPTED`

4. `step_name`
- Query: `label_values(saga_step_duration_success_seconds_count{saga_name=~"$saga_name",saga_version=~"$saga_version"}, step_name)`
- Include all option: `.*`
- Multi-value: enabled

## Dashboard Layout

## 1) Executive Health Row

### Panel: Terminal Throughput by Definition
- Type: Time series (stacked)
- Purpose: finished saga rate by definition and status.
- Query:
```promql
sum by (saga_name, saga_version, final_status) (
  rate(saga_duration_seconds_count{saga_name=~"$saga_name", saga_version=~"$saga_version", final_status=~"$final_status"}[$__rate_interval])
)
```

### Panel: Success Ratio by Definition
- Type: Time series (percent)
- Purpose: SLO-like signal for execution outcome quality.
- Query:
```promql
(
  sum by (saga_name, saga_version) (
    rate(saga_duration_seconds_count{saga_name=~"$saga_name", saga_version=~"$saga_version", final_status="SUCCEEDED"}[$__rate_interval])
  )
/
  clamp_min(
    sum by (saga_name, saga_version) (
      rate(saga_duration_seconds_count{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
    ),
    1e-9
  )
) * 100
```
- Suggested thresholds: green `>=99`, yellow `>=95`, red `<95`.

### Panel: Failure/Corruption Ratio by Definition
- Type: Time series (percent)
- Query:
```promql
(
  sum by (saga_name, saga_version) (
    rate(saga_duration_seconds_count{saga_name=~"$saga_name", saga_version=~"$saga_version", final_status=~"FAILED|CORRUPTED"}[$__rate_interval])
  )
/
  clamp_min(
    sum by (saga_name, saga_version) (
      rate(saga_duration_seconds_count{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
    ),
    1e-9
  )
) * 100
```

## 2) Latency Row (Definition-level)

### Panel: Saga p50 / p95 / p99 Duration
- Type: Time series
- Purpose: end-to-end latency percentiles by definition.
- p50 query:
```promql
histogram_quantile(0.50,
  sum by (le, saga_name, saga_version) (
    rate(saga_duration_seconds_bucket{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
  )
)
```
- p95 query: same with `0.95`
- p99 query: same with `0.99`

### Panel: Average Saga Duration
- Type: Time series
- Query:
```promql
sum by (saga_name, saga_version) (
  rate(saga_duration_seconds_sum{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
)
/
clamp_min(
  sum by (saga_name, saga_version) (
    rate(saga_duration_seconds_count{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
  ),
  1e-9
)
```

### Panel: Slowest Definitions (Top 10 p95)
- Type: Bar gauge or table
- Query:
```promql
topk(10,
  histogram_quantile(0.95,
    sum by (le, saga_name, saga_version) (
      rate(saga_duration_seconds_bucket[$__rate_interval])
    )
  )
)
```

## 3) Step Performance Row

### Panel: Step p95 by Definition and Step Name
- Type: Time series
- Query:
```promql
histogram_quantile(0.95,
  sum by (le, saga_name, saga_version, step_name) (
    rate(saga_step_duration_success_seconds_bucket{
      saga_name=~"$saga_name",
      saga_version=~"$saga_version",
      step_name=~"$step_name"
    }[$__rate_interval])
  )
)
```

### Panel: Step Average Duration
- Type: Table
- Query:
```promql
sum by (saga_name, saga_version, step_name) (
  rate(saga_step_duration_success_seconds_sum{saga_name=~"$saga_name", saga_version=~"$saga_version", step_name=~"$step_name"}[$__rate_interval])
)
/
clamp_min(
  sum by (saga_name, saga_version, step_name) (
    rate(saga_step_duration_success_seconds_count{saga_name=~"$saga_name", saga_version=~"$saga_version", step_name=~"$step_name"}[$__rate_interval])
  ),
  1e-9
)
```

### Panel: Hottest Steps by Throughput
- Type: Bar chart
- Query:
```promql
topk(15,
  sum by (saga_name, saga_version, step_name) (
    rate(saga_step_duration_success_seconds_count{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
  )
)
```

## 4) Queue and Runtime Pressure Row

### Panel: In-memory Queue Size
- Type: Time series
- Query:
```promql
saga_inmemory_queue_size
```

### Panel: Enqueue vs Dequeue Rate
- Type: Time series
- Query A:
```promql
sum by (saga_name, saga_version) (
  rate(saga_enqueue_total{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
)
```
- Query B:
```promql
sum by (saga_name, saga_version) (
  rate(saga_dequeue_total{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
)
```
- Interpretation: sustained `enqueue > dequeue` can indicate backlog risk.

### Panel: Processed, Failed, Retried, Rate-Limited Rates
- Type: Time series (multi-series)
- Queries:
```promql
sum by (saga_name, saga_version, outcome) (
  rate(saga_processed_total{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
)
```
```promql
sum by (saga_name, saga_version, reason) (
  rate(saga_failed_total{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
)
```
```promql
sum by (saga_name, saga_version) (
  rate(saga_retried_total{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
)
```
```promql
sum by (saga_name, saga_version) (
  rate(saga_rate_limited_total{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
)
```

## 5) Definition Deep-Dive Row
Use dashboard links or panel drill-down with selected `saga_name` + `saga_version`.

### Panel: Terminal Status Distribution (pie)
```promql
sum by (final_status) (
  increase(saga_duration_seconds_count{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__range])
)
```

### Panel: End-to-End Duration Heatmap
- Type: Heatmap
- Query:
```promql
sum by (le) (
  rate(saga_duration_seconds_bucket{saga_name=~"$saga_name", saga_version=~"$saga_version"}[$__rate_interval])
)
```

### Panel: Step Duration Heatmap (selected step)
- Type: Heatmap
- Query:
```promql
sum by (le) (
  rate(saga_step_duration_success_seconds_bucket{saga_name=~"$saga_name", saga_version=~"$saga_version", step_name=~"$step_name"}[$__rate_interval])
)
```

## Alert Recommendations

1. High failure ratio by definition
- Condition: failure ratio > 5% for 10m
```promql
(
  sum by (saga_name, saga_version) (rate(saga_duration_seconds_count{final_status=~"FAILED|CORRUPTED"}[10m]))
/
  clamp_min(sum by (saga_name, saga_version) (rate(saga_duration_seconds_count[10m])), 1e-9)
) > 0.05
```

2. Saga latency regression (p95)
- Condition: p95 > SLO target for 15m
```promql
histogram_quantile(0.95,
  sum by (le, saga_name, saga_version) (rate(saga_duration_seconds_bucket[15m]))
) > 2
```
(Example threshold `2` seconds; tune by saga.)

3. Queue pressure growing
- Condition: enqueue > dequeue over sustained window
```promql
sum(rate(saga_enqueue_total[10m])) > sum(rate(saga_dequeue_total[10m]))
```

4. Rate limiter saturation
```promql
rate(saga_rate_limited_total[10m]) > 0
```

## Definition-level Health Coverage
Definition-level filtering/grouping is now available across counters and histograms through `saga_name` and `saga_version` labels. This enables team-level ownership views for throughput, failures, retries, and latency on a single dashboard.

## Suggested Dashboard Folder Structure
- `Trama / Overview`
- `Trama / Definition Deep Dive`
- `Trama / Runtime & Capacity`
- `Trama / SLO & Alerts`

## Implementation Notes
- Use Grafana panel units:
  - durations: `s`
  - rates: `ops/s`
  - ratios: `%`
- Enable exemplars if your Prometheus/Grafana stack supports OpenTelemetry trace correlation.
- Keep legend format compact, e.g. `{{saga_name}}/{{saga_version}}`.
