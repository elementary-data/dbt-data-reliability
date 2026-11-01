import json
from datetime import datetime, time, timedelta
from itertools import pairwise
from typing import Any, Dict, List, Optional

import pytest
from data_generator import DATE_FORMAT
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
VALUE_COLUMN = "amount"
OTHER_VALUE_COLUMN = "other_amount"
DBT_TEST_NAME = "elementary.metric_stability"

BASE_AMOUNT = 100
OTHER_BASE_AMOUNT = 500
DAYS_OF_HISTORY = 6

# A min_bucket_age of one day means buckets older than a day are checked, and the
# derived observation window (twice the age) keeps them being measured, so a
# bucket two days old is both settled and still under observation.
SETTLED_DAYS_AGO = 2
UNSETTLED_DAYS_AGO = 1

BASE_ARGS: Dict[str, Any] = {
    "columns": [VALUE_COLUMN],
    "metrics": ["sum"],
    "timestamp_column": TIMESTAMP_COLUMN,
    "time_bucket": {"period": "day", "count": 1},
    "min_bucket_age": {"count": 1, "period": "day"},
}


def _rows(
    restatements: Optional[Dict[int, float]] = None,
    other_restatements: Optional[Dict[int, float]] = None,
) -> List[Dict[str, Any]]:
    """One row per day, midday so it lands unambiguously inside a daily bucket."""
    restatements = restatements or {}
    other_restatements = other_restatements or {}
    utc_today = datetime.utcnow().date()
    rows = []
    for days_ago in range(1, DAYS_OF_HISTORY + 1):
        timestamp = datetime.combine(utc_today - timedelta(days=days_ago), time(12, 0))
        rows.append(
            {
                TIMESTAMP_COLUMN: timestamp.strftime(DATE_FORMAT),
                VALUE_COLUMN: restatements.get(days_ago, BASE_AMOUNT),
                OTHER_VALUE_COLUMN: other_restatements.get(days_ago, OTHER_BASE_AMOUNT),
            }
        )
    return rows


def _run(dbt_project: DbtProject, test_id: str, data, **overrides) -> str:
    result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        {**BASE_ARGS, **overrides},
        data=data,
        test_vars={"enable_elementary_test_materialization": True},
    )
    return result["status"]


def _bucket_values(
    dbt_project: DbtProject, test_id: str, column_name: str = VALUE_COLUMN
) -> Dict[Any, List[float]]:
    """Measured values per bucket, oldest measurement first.

    Asserting on these rather than only on pass/fail means a wrong baseline or a
    wrong sign in the comparison cannot slip through.
    """
    metrics = dbt_project.read_table(
        "data_monitoring_metrics",
        where=(
            f"full_table_name LIKE '%{test_id.upper()}' "
            f"and metric_name = 'sum' "
            f"and lower(column_name) = '{column_name}'"
        ),
    )
    by_bucket: Dict[Any, List[Any]] = {}
    for metric in metrics:
        by_bucket.setdefault(str(metric["bucket_end"]), []).append(
            (str(metric["updated_at"]), float(metric["metric_value"]))
        )
    return {
        bucket: [value for _, value in sorted(measurements)]
        for bucket, measurements in by_bucket.items()
    }


def _restated_bucket(values: Dict[Any, List[float]]) -> List[float]:
    """The one bucket whose measurements are not all identical."""
    moved = [
        measurements for measurements in values.values() if len(set(measurements)) > 1
    ]
    assert len(moved) == 1, f"expected exactly one bucket to move, got {moved}"
    return moved[0]


def test_metric_stability_detects_restated_settled_value(
    test_id: str, dbt_project: DbtProject
):
    baseline = _rows()
    args = {"change_since": ["last_check"]}

    # The first run only establishes an initial measurement, so there is nothing
    # to compare against yet.
    assert _run(dbt_project, test_id, baseline, **args) == "pass"

    # The second measures the same buckets again and the values are unchanged.
    assert _run(dbt_project, test_id, baseline, **args) == "pass"

    # Rewriting the value of an already-settled bucket is what the test exists to
    # catch, even though the value itself is unremarkable next to other days.
    restated = _rows({SETTLED_DAYS_AGO: BASE_AMOUNT * 2})
    assert _run(dbt_project, test_id, restated, **args) == "fail"

    # The bucket the test flagged must be the one that actually moved, and by
    # the amount restated, so a wrong baseline cannot pass unnoticed.
    measurements = _restated_bucket(_bucket_values(dbt_project, test_id))
    assert measurements[0] == BASE_AMOUNT
    assert measurements[-1] == BASE_AMOUNT * 2


def test_metric_stability_first_check_catches_gradual_drift(
    test_id: str, dbt_project: DbtProject
):
    """Drift too small to trip the threshold on any single step, but not overall.

    This is the case that justifies having 'first_check' at all: comparing only
    against the previous measurement never sees it.
    """
    args = {"change_since": ["first_check"], "max_change_percent": 15}
    assert _run(dbt_project, test_id, _rows(), **args) == "pass"

    # +10% against the original value: under the threshold either way.
    assert _run(dbt_project, test_id, _rows({SETTLED_DAYS_AGO: 110}), **args) == "pass"

    # A further +9% step, still under the threshold on its own, but now 20% away
    # from where the bucket started.
    assert _run(dbt_project, test_id, _rows({SETTLED_DAYS_AGO: 120}), **args) == "fail"

    # Each step is under the threshold; only the distance from the first
    # measurement crosses it.
    measurements = _restated_bucket(_bucket_values(dbt_project, test_id))
    assert measurements[0] == BASE_AMOUNT
    assert measurements[-1] == 120
    steps = [
        later - earlier for earlier, later in pairwise(measurements) if later != earlier
    ]
    assert all(step / BASE_AMOUNT * 100 < 15 for step in steps), steps


def test_metric_stability_last_check_ignores_gradual_drift(
    test_id: str, dbt_project: DbtProject
):
    """The same drift, compared only against the previous run, stays invisible."""
    args = {"change_since": ["last_check"], "max_change_percent": 15}
    assert _run(dbt_project, test_id, _rows(), **args) == "pass"
    assert _run(dbt_project, test_id, _rows({SETTLED_DAYS_AGO: 110}), **args) == "pass"
    assert _run(dbt_project, test_id, _rows({SETTLED_DAYS_AGO: 120}), **args) == "pass"


def test_metric_stability_records_history_across_runs(
    test_id: str, dbt_project: DbtProject
):
    """Every run must add a measurement, not replace the previous one.

    The comparison has nothing to compare unless earlier measurements survive in
    data_monitoring_metrics. On adapters that roll back the test transaction an
    INSERT-populated metrics table is discarded before the on-run-end flush, and
    the test then passes forever without ever recording anything. Asserting on
    the accumulating row counts catches that directly, where a pass/fail
    assertion cannot tell "nothing changed" from "nothing was measured".
    """
    baseline = _rows()
    counts = []
    for _ in range(3):
        assert _run(dbt_project, test_id, baseline) == "pass"
        values = _bucket_values(dbt_project, test_id)
        assert values, "no metrics were recorded at all"
        counts.append(sum(len(m) for m in values.values()))

    assert counts[0] > 0, counts
    assert counts[1] > counts[0], counts
    assert counts[2] > counts[1], counts

    settled = [m for m in values.values() if len(m) == 3]
    assert settled, f"no bucket was measured on all three runs: {values}"


def test_metric_stability_detects_restatement_with_weekly_buckets(
    test_id: str, dbt_project: DbtProject
):
    """A bucket longer than a day must still get a stable identity across runs.

    The bucket grid is anchored on a value that moves by a day between runs, so
    without snapping the anchor to the bucket period every measurement lands on
    a fresh surrogate id, no bucket is ever measured twice and the test silently
    never fires.
    """
    args = {
        "time_bucket": {"period": "week", "count": 1},
        "min_bucket_age": {"count": 1, "period": "week"},
    }
    restate_days_ago = 16

    def weekly_rows(restated=None):
        utc_today = datetime.utcnow().date()
        rows = []
        for days_ago in range(1, 36):
            timestamp = datetime.combine(
                utc_today - timedelta(days=days_ago), time(12, 0)
            )
            rows.append(
                {
                    TIMESTAMP_COLUMN: timestamp.strftime(DATE_FORMAT),
                    VALUE_COLUMN: (
                        restated
                        if restated and days_ago == restate_days_ago
                        else BASE_AMOUNT
                    ),
                    OTHER_VALUE_COLUMN: OTHER_BASE_AMOUNT,
                }
            )
        return rows

    assert _run(dbt_project, test_id, weekly_rows(), **args) == "pass"
    assert _run(dbt_project, test_id, weekly_rows(), **args) == "pass"
    assert (
        _run(dbt_project, test_id, weekly_rows(restated=BASE_AMOUNT * 2), **args)
        == "fail"
    )


def test_metric_stability_rejects_multi_step_buckets(
    test_id: str, dbt_project: DbtProject
):
    """A count > 1 bucket cannot be given a stable identity, so it must raise."""
    result = _run(
        dbt_project,
        test_id,
        _rows(),
        time_bucket={"period": "day", "count": 3},
    )
    assert result == "error"


# Explicit ids: the value ends up in the seed relation name, and a "." in one
# is rejected by the Hive metastore behind Trino.
@pytest.mark.parametrize(
    "days_back", [pytest.param(1, id="whole"), pytest.param(0.5, id="fractional")]
)
def test_metric_stability_rejects_too_short_window(
    test_id: str, dbt_project: DbtProject, days_back
):
    """A window too short to outlast settling must raise, not pass vacuously.

    The query truncates days_back to whole days, so a fractional value has to be
    rejected on its effective size rather than on the number as written.
    """
    result = _run(
        dbt_project,
        test_id,
        _rows(),
        min_bucket_age={"count": 1, "period": "hour"},
        days_back=days_back,
    )
    assert result == "error"


def test_metric_stability_ignores_measurements_taken_while_settling(
    test_id: str, dbt_project: DbtProject
):
    """A bucket's own settling must not become the 'first_check' baseline.

    The first measurements of a bucket are taken while late records are still
    arriving, which is the period min_bucket_age exists to exclude. If they are
    used as the baseline, every later comparison carries that settling as a
    permanent offset and real drift is buried under it.
    """
    args = {"change_since": ["first_check"], "max_change_percent": 15}
    assert _run(dbt_project, test_id, _rows(), **args) == "pass"

    # Backdate this run's measurements into the settling window and move their
    # values far away. Were they still eligible as a baseline, the next run
    # would compare 100 against 10 and report a 900% change.
    if dbt_project.target == "clickhouse":
        # ClickHouse only supports updates as (asynchronous) mutations.
        update_clause = "ALTER TABLE {{ ref('data_monitoring_metrics') }} UPDATE"
        update_suffix = "SETTINGS mutations_sync = 1"
    else:
        update_clause = "UPDATE {{ ref('data_monitoring_metrics') }} SET"
        update_suffix = ""
    dbt_project.run_query(
        f"""
        {update_clause} metric_value = 10, updated_at = bucket_end
        WHERE full_table_name LIKE '%{test_id.upper()}'
        AND metric_name = 'sum'
        {update_suffix}
        """
    )

    assert _run(dbt_project, test_id, _rows(), **args) == "pass"


# A ratio sits on a fixed scale, so the move that matters is the absolute one.
# 45.30% dropping to 45.10% is 20 basis points, which is what max_change_absolute
# expresses directly.
BASE_MARGIN = 45.30
RATIO_ARGS: Dict[str, Any] = {"metrics": ["average"]}


def test_metric_stability_absolute_threshold_on_a_ratio(
    test_id: str, dbt_project: DbtProject
):
    """An absolute threshold means the same move at every level of the ratio."""
    args = {**RATIO_ARGS, "max_change_absolute": 0.20}

    assert (
        _run(dbt_project, test_id, _rows({d: BASE_MARGIN for d in range(1, 7)}), **args)
        == "pass"
    )
    assert (
        _run(dbt_project, test_id, _rows({d: BASE_MARGIN for d in range(1, 7)}), **args)
        == "pass"
    )

    # 10bp, inside the tolerance.
    rows = {d: BASE_MARGIN for d in range(1, 7)}
    rows[SETTLED_DAYS_AGO] = 45.20
    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"

    # A further 30bp, outside it.
    rows[SETTLED_DAYS_AGO] = 44.90
    assert _run(dbt_project, test_id, _rows(rows), **args) == "fail"


def test_metric_stability_both_thresholds_must_be_exceeded(
    test_id: str, dbt_project: DbtProject
):
    """With both set the absolute one is a floor under the relative one.

    A small absolute move on a small base looks large in relative terms, which
    is exactly the noise an absolute floor is meant to suppress.
    """
    args = {**RATIO_ARGS, "max_change_percent": 1.0, "max_change_absolute": 0.20}
    low = 5.00

    assert (
        _run(dbt_project, test_id, _rows({d: low for d in range(1, 7)}), **args)
        == "pass"
    )
    assert (
        _run(dbt_project, test_id, _rows({d: low for d in range(1, 7)}), **args)
        == "pass"
    )

    # 0.10 is 2% of a base of 5, so the relative threshold alone would fire. It
    # is under the absolute floor, so together they do not.
    rows = {d: low for d in range(1, 7)}
    rows[SETTLED_DAYS_AGO] = 5.10
    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"

    # 0.50 clears the floor and is still well over 1%, so both agree.
    rows[SETTLED_DAYS_AGO] = 5.60
    assert _run(dbt_project, test_id, _rows(rows), **args) == "fail"


def test_metric_stability_absolute_tolerates_zero_baseline(
    test_id: str, dbt_project: DbtProject
):
    """Relative change is undefined at zero; absolute change is not.

    With only a relative threshold any move away from exactly zero has to fail,
    because the division has no meaning there. An absolute threshold is well
    defined at zero, so a metric legitimately sitting at 0 is held to the same
    tolerance as everything else.
    """
    args = {**RATIO_ARGS, "max_change_absolute": 5}
    rows = {d: 0 for d in range(1, 7)}

    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"
    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"

    # A move away from zero inside the tolerance. A relative threshold would
    # have had to fail this.
    rows[SETTLED_DAYS_AGO] = 2
    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"

    # And the threshold still bites beyond it.
    rows[SETTLED_DAYS_AGO] = 10
    assert _run(dbt_project, test_id, _rows(rows), **args) == "fail"


def test_metric_stability_both_detect_zero_baseline(
    test_id: str, dbt_project: DbtProject
):
    """At a zero baseline the absolute threshold has to decide on its own.

    Relative change has no meaning at zero, so a relative conjunct of the form
    "baseline != 0 and ..." is false there. Combined with the absolute one under
    AND it would suppress every zero-baseline restatement, so adding an absolute
    threshold as a noise floor would silently switch that detection off.
    """
    args = {**RATIO_ARGS, "max_change_percent": 10, "max_change_absolute": 5}
    rows = {d: 0 for d in range(1, 7)}

    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"
    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"

    rows[SETTLED_DAYS_AGO] = 1000
    assert _run(dbt_project, test_id, _rows(rows), **args) == "fail"


def test_metric_stability_unset_threshold_still_fails_any_change(
    test_id: str, dbt_project: DbtProject
):
    """An unconfigured threshold means zero, not "some small number".

    max_change_percent defaults to none internally so that unset is
    distinguishable from an explicit zero. The other tests restate by a factor
    of two, so they would still pass if unset silently resolved to anything
    under 100%. This pins a change far below any plausible accidental default.
    """
    assert _run(dbt_project, test_id, _rows()) == "pass"
    assert _run(dbt_project, test_id, _rows()) == "pass"

    # 0.1% of the base amount, which must still fail with nothing configured.
    assert _run(dbt_project, test_id, _rows({SETTLED_DAYS_AGO: 100.1})) == "fail"


def test_metric_stability_absolute_on_both_baselines(
    test_id: str, dbt_project: DbtProject
):
    """Each baseline gets its own threshold pair, OR'd together."""
    args = {
        **RATIO_ARGS,
        "change_since": ["last_check", "first_check"],
        "max_change_absolute": 0.20,
    }
    rows = {d: BASE_MARGIN for d in range(1, 7)}

    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"
    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"

    # 15bp per step: under the tolerance against the previous measurement, but
    # first_check sees the total move from where the bucket started.
    rows[SETTLED_DAYS_AGO] = 45.15
    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"
    rows[SETTLED_DAYS_AGO] = 45.00
    assert _run(dbt_project, test_id, _rows(rows), **args) == "fail"


@pytest.mark.parametrize(
    "overrides",
    [
        {"max_change_absolute": -1},
        {"max_change_absolute": "0.20"},
        {"max_change_absolute": True},
        {"max_change_absolute": 0.00000000001},
        {"max_change_percent": -1},
        {"max_change_percent": "25"},
        {"max_change_percent": True},
        {"max_change_percent": 0.0000000001},
    ],
)
def test_metric_stability_rejects_bad_thresholds(
    test_id: str, dbt_project: DbtProject, overrides: Dict[str, Any]
):
    """Every threshold value that cannot mean what it says must raise.

    Booleans matter because bool subclasses int in python, so Jinja's `number`
    test accepts True and would render it as a threshold of 1. The tiny values
    matter because one is clamped to the noise floor and the other rounds to
    zero, either of which would apply a threshold nobody asked for.
    """
    assert _run(dbt_project, test_id, _rows(), **overrides) == "error"


# metric_value is stored through dbt's type_float, which is single precision on
# some adapters and double on others: the same subtraction gives
# 0.20000000000000284 on Postgres and 0.20000076293945312 on DuckDB and
# ClickHouse. Where it is single precision the storage error is four orders of
# magnitude above the tolerance below, so a move sitting exactly on the threshold
# is not decidable and the assertion would be meaningless rather than wrong. The
# list errs towards over-skipping; the behaviour under test is the tolerance, and
# that is exercised wherever metric_value is double precision.
@pytest.mark.skip_targets(
    [
        "clickhouse",
        "duckdb",
        "spark",
        "fabricspark",
        "databricks_catalog",
        "athena",
        "trino",
    ]
)
def test_metric_stability_absolute_float_boundary(
    test_id: str, dbt_project: DbtProject
):
    """A move of exactly the threshold must not fire, in either direction.

    Decimal thresholds have no exact float representation, so two moves that are
    both 0.20 in decimal land either side of it: 45.30 -> 45.10 comes out as
    0.19999999999999574 and 45.30 -> 45.50 as 0.20000000000000284. Without a
    tolerance the second fires and the first does not, which is the customer's
    own 20bp example behaving differently depending on which way it moved.
    """
    args = {**RATIO_ARGS, "max_change_absolute": 0.20}
    rows = {d: BASE_MARGIN for d in range(1, 7)}

    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"
    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"

    # Exactly 20bp up, the direction that rounds against us.
    rows[SETTLED_DAYS_AGO] = 45.50
    assert _run(dbt_project, test_id, _rows(rows), **args) == "pass"

    # 30bp further, comfortably past the threshold.
    rows[SETTLED_DAYS_AGO] = 45.80
    assert _run(dbt_project, test_id, _rows(rows), **args) == "fail"


def test_metric_stability_ignores_unsettled_buckets(
    test_id: str, dbt_project: DbtProject
):
    baseline = _rows()
    assert _run(dbt_project, test_id, baseline) == "pass"
    assert _run(dbt_project, test_id, baseline) == "pass"

    # Recent data is expected to keep moving as late records arrive, so a change
    # inside min_bucket_age must not be reported.
    restated = _rows({UNSETTLED_DAYS_AGO: BASE_AMOUNT * 2})
    assert _run(dbt_project, test_id, restated) == "pass"


def test_metric_stability_tolerates_change_within_threshold(
    test_id: str, dbt_project: DbtProject
):
    args = {"max_change_percent": 25}
    assert _run(dbt_project, test_id, _rows(), **args) == "pass"
    assert _run(dbt_project, test_id, _rows(), **args) == "pass"

    # A 10% restatement sits under the 25% tolerance and should be allowed, which
    # is what makes one relative threshold usable across metrics of very
    # different magnitudes.
    within = _rows({SETTLED_DAYS_AGO: int(BASE_AMOUNT * 1.1)})
    assert _run(dbt_project, test_id, within, **args) == "pass"

    beyond = _rows({SETTLED_DAYS_AGO: BASE_AMOUNT * 2})
    assert _run(dbt_project, test_id, beyond, **args) == "fail"


def test_metric_stability_detects_restatement_in_any_column(
    test_id: str, dbt_project: DbtProject
):
    """Every monitored column is compared against this run's own measurements.

    Collecting per column into separate tables would leave all but the last
    column comparing against the previous run, so a restatement in an earlier
    column would surface a run late.
    """
    args = {"columns": [VALUE_COLUMN, OTHER_VALUE_COLUMN]}
    baseline = _rows()
    assert _run(dbt_project, test_id, baseline, **args) == "pass"
    assert _run(dbt_project, test_id, baseline, **args) == "pass"

    # Restate the first of the two columns, which is the one a per-column table
    # would have left stale.
    restated = _rows({SETTLED_DAYS_AGO: BASE_AMOUNT * 2})
    assert _run(dbt_project, test_id, restated, **args) == "fail"

    # The restated column moved, and the other one did not.
    measurements = _restated_bucket(_bucket_values(dbt_project, test_id))
    assert measurements[0] == BASE_AMOUNT
    assert measurements[-1] == BASE_AMOUNT * 2
    other = _bucket_values(dbt_project, test_id, OTHER_VALUE_COLUMN)
    assert all(len(set(m)) == 1 for m in other.values()), other


def _samples(dbt_project: DbtProject, test_id: str):
    test_id = test_id.replace("[", "_").replace("]", "_")
    return [
        {key.lower(): value for key, value in json.loads(row["result_row"]).items()}
        for row in dbt_project.run_query(dbt_project.samples_query(test_id))
    ]


@pytest.mark.parametrize("baseline", ["last_check", "first_check"])
def test_metric_stability_quoted_column_details(
    test_id: str, dbt_project: DbtProject, baseline: str
):
    args = {"columns": ['"amount"'], "change_since": [baseline]}
    assert _run(dbt_project, test_id, _rows(), **args) == "pass"
    assert _run(dbt_project, test_id, _rows({SETTLED_DAYS_AGO: 200}), **args) == "fail"
    samples = _samples(dbt_project, test_id)
    assert len(samples) == 1
    sample = samples[0]
    assert sample["change_type"] == "value_changed"
    assert float(sample["metric_value"]) == 200
    assert float(sample["previous_value"]) == 100
    assert float(sample["initial_value"]) == 100
    assert sample["bucket_end"]
    assert sample["previous_measured_at"]
    assert sample["initial_measured_at"]
    # A repeated corrected value is accepted only by the moving baseline.
    expected = "pass" if baseline == "last_check" else "fail"
    assert (
        _run(dbt_project, test_id, _rows({SETTLED_DAYS_AGO: 200}), **args) == expected
    )


@pytest.mark.parametrize("dimensions", [[], ["other_amount"]])
def test_metric_stability_reports_disappearing_bucket(
    test_id: str, dbt_project: DbtProject, dimensions
):
    assert _run(dbt_project, test_id, _rows(), dimensions=dimensions) == "pass"
    rows = _rows()
    del rows[SETTLED_DAYS_AGO - 1]
    assert _run(dbt_project, test_id, rows, dimensions=dimensions) == "fail"
    samples = _samples(dbt_project, test_id)
    assert len(samples) == 1
    assert samples[0]["change_type"] == "missing_bucket"
    assert samples[0]["metric_value"] is None
    assert float(samples[0]["previous_value"]) == BASE_AMOUNT
    # Absence is not accepted as a new numeric baseline on the next run.
    assert _run(dbt_project, test_id, rows, dimensions=dimensions) == "fail"
    assert _run(dbt_project, test_id, _rows(), dimensions=dimensions) == "pass"


def test_metric_stability_ignores_disappearance_off_window(
    test_id: str, dbt_project: DbtProject
):
    assert _run(dbt_project, test_id, _rows(), days_back=10) == "pass"
    # Keep only the recent rows; historical measurements still exist but the
    # default window no longer covers the deleted older buckets.
    assert _run(dbt_project, test_id, _rows()[:3]) == "pass"


def test_metric_stability_reports_disappearing_dimension(
    test_id: str, dbt_project: DbtProject
):
    rows = _rows()
    rows.append({**rows[SETTLED_DAYS_AGO - 1], OTHER_VALUE_COLUMN: 999})
    args = {"dimensions": [OTHER_VALUE_COLUMN]}
    assert _run(dbt_project, test_id, rows, **args) == "pass"
    assert _run(dbt_project, test_id, _rows(), **args) == "fail"
    samples = _samples(dbt_project, test_id)
    assert len(samples) == 1
    assert samples[0]["change_type"] == "missing_bucket"
    assert "999" in str(samples[0]["dimension_value"])


def test_metric_stability_skips_unscanned_buckets(
    test_id: str, dbt_project: DbtProject
):
    assert _run(dbt_project, test_id, _rows(), days_back=6) == "pass"
    # Sources use the incremental backfill window. Older buckets have history
    # and remain in days_back, but are not rescanned with backfill_days=3.
    assert (
        _run(dbt_project, test_id, _rows()[:3], days_back=6, backfill_days=3) == "pass"
    )
