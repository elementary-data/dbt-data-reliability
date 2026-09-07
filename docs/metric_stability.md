# Metric stability

`elementary.metric_stability` detects changes to a time bucket's own previously
measured aggregates after the bucket has settled. Use it for historical revenue,
costs, or other measures that should stop changing after late data has arrived.
Ordinary anomaly detection compares different periods; it may notice some effects
of a restatement, but does not directly enforce this expectation.

```yaml
models:
  - name: orders
    tests:
      - elementary.metric_stability:
          columns: [cost_amount, revenue_amount]
          metrics: [sum]
          timestamp_column: order_ts
          time_bucket: {count: 1, period: day}
          min_bucket_age: {count: 4, period: week}
          days_back: 90
          change_since: [first_check]
          max_change_percent: 1
```

Choose a business/event timestamp whose historical periods you want to protect.
A row's ingestion or last-modified timestamp can move it between buckets when it
is updated, which answers a different question.

## Coverage and cost

`min_bucket_age` measures time since the bucket ended. Measurements taken before
that age are excluded from both baselines. The first eligible measurement only
establishes a baseline; a pass at that point does not verify historical stability.

`days_back` bounds the observation window. The example protects daily buckets
roughly 28 to 90 days old, not all historical data. Corrections outside that window
are not detected. Incremental models and sources also use `backfill_days` to
control remeasurement; it defaults to `days_back`. An explicitly shorter backfill
window reduces coverage to the buckets actually scanned on that run.

Without an explicit window, the test derives one from the settling age (roughly
twice the age, with room for whole buckets). This is a convenience default, not a
business retention policy. Set the window to cover the corrections you care about
and run frequently enough to measure each eligible bucket more than once.
Longer windows increase rescanning and metric-history storage costs.

## Baselines and legitimate corrections

- `last_check` compares against the previous eligible measurement. With zero
  tolerance, 100 -> 120 fails; a subsequent 120 passes. New measurements become
  the baseline automatically, including measurements from failing runs.
- `first_check` compares against the earliest retained eligible measurement.
  It catches cumulative drift: 100 -> 110 -> 120 exceeds a 15% threshold overall,
  although each step is smaller. A corrected 120 continues failing against 100
  until it returns within tolerance or the bucket leaves coverage.
- Selecting both fails if either comparison exceeds the threshold.

There is no explicit accept/reset-baseline operation in this version. Choose
`last_check` when changes should be reported once and then automatically accepted.
Choose `first_check` when continued deviation should remain a failure. Switching
to `last_check` changes the policy; it does not reset `first_check`. Account for
history retention and cleanup: the baseline is the earliest *retained*
measurement, not an immutable approved snapshot. Do not use this test as a
substitute for an auditable financial close or an immutable snapshot.

## Failure details

With Elementary’s test materialization enabled, stored samples include the bucket, column, metric, dimensions, current and baseline
values, measurement timestamps, and absolute and percentage deltas. Normal
Elementary sample limits and privacy controls apply.

`change_type: value_changed` reports numeric movement above the configured
percentage threshold. `max_change_percent: 1` means 1%, not 100%; the default is
zero with a tiny relative floor to suppress floating-point aggregation noise.
Movement away from a zero baseline always fails because relative change is
undefined there.

`change_type: missing_bucket` means a previously measured bucket or dimension has
no current metric in a window the test actually rescanned. It fails regardless of
the percentage tolerance, reports a NULL current value and the last observed
value, and remains a failure while missing and within coverage. It does not invent
a zero for aggregates such as average or minimum.

Stable aggregates do not guarantee unchanged source rows. Offsetting changes can
cancel in a sum; pair this test with row-level checks when record immutability is
the requirement.
