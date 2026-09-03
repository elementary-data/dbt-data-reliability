from datetime import datetime, time, timedelta
from itertools import pairwise
from typing import Any, Dict, List, Optional

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
    restatements: Optional[Dict[int, int]] = None,
    other_restatements: Optional[Dict[int, int]] = None,
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
        test_id, DBT_TEST_NAME, {**BASE_ARGS, **overrides}, data=data
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
