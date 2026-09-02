from datetime import datetime, time, timedelta
from typing import Any, Dict, List

from data_generator import DATE_FORMAT
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
VALUE_COLUMN = "amount"
DBT_TEST_NAME = "elementary.metric_stability"

BASE_AMOUNT = 100
DAYS_OF_HISTORY = 6

# min_bucket_age of one day means buckets older than a day are checked, and the
# derived backfill window (twice the age) keeps them being re-measured, so a
# bucket two days old is both settled and still under observation.
SETTLED_DAYS_AGO = 2
UNSETTLED_DAYS_AGO = 1

DBT_TEST_ARGS: Dict[str, Any] = {
    "columns": [VALUE_COLUMN],
    "metrics": ["sum"],
    "timestamp_column": TIMESTAMP_COLUMN,
    "time_bucket": {"period": "day", "count": 1},
    "days_back": 7,
    "change_since": ["last_check", "first_check"],
    "min_bucket_age": {"count": 1, "period": "day"},
}


def _rows(restatements: Dict[int, int]) -> List[Dict[str, Any]]:
    """One row per day, midday so it lands unambiguously inside a daily bucket."""
    utc_today = datetime.utcnow().date()
    rows = []
    for days_ago in range(1, DAYS_OF_HISTORY + 1):
        timestamp = datetime.combine(utc_today - timedelta(days=days_ago), time(12, 0))
        rows.append(
            {
                TIMESTAMP_COLUMN: timestamp.strftime(DATE_FORMAT),
                VALUE_COLUMN: restatements.get(days_ago, BASE_AMOUNT),
            }
        )
    return rows


def _run(dbt_project: DbtProject, test_id: str, data, **overrides) -> str:
    args = {**DBT_TEST_ARGS, **overrides}
    result = dbt_project.test(test_id, DBT_TEST_NAME, args, data=data)
    return result["status"]


def test_metric_stability_detects_restated_settled_value(
    test_id: str, dbt_project: DbtProject
):
    baseline = _rows({})

    # First run only establishes an initial measurement, so there is nothing to
    # compare against yet.
    assert _run(dbt_project, test_id, baseline) == "pass"

    # Second run measures the same buckets again and the values are unchanged.
    assert _run(dbt_project, test_id, baseline) == "pass"

    # Rewriting the value of an already-settled bucket is what the test exists
    # to catch, even though the value itself is unremarkable next to other days.
    restated = _rows({SETTLED_DAYS_AGO: BASE_AMOUNT * 2})
    assert _run(dbt_project, test_id, restated) == "fail"


def test_metric_stability_ignores_unsettled_buckets(
    test_id: str, dbt_project: DbtProject
):
    baseline = _rows({})
    assert _run(dbt_project, test_id, baseline) == "pass"
    assert _run(dbt_project, test_id, baseline) == "pass"

    # Recent data is expected to keep moving as late records arrive, so a change
    # inside min_bucket_age must not be reported.
    restated = _rows({UNSETTLED_DAYS_AGO: BASE_AMOUNT * 2})
    assert _run(dbt_project, test_id, restated) == "pass"


def test_metric_stability_tolerates_change_within_threshold(
    test_id: str, dbt_project: DbtProject
):
    baseline = _rows({})
    assert _run(dbt_project, test_id, baseline, max_change_percent=25) == "pass"
    assert _run(dbt_project, test_id, baseline, max_change_percent=25) == "pass"

    # A 10% restatement sits under the 25% tolerance and should be allowed,
    # which is what makes one relative threshold usable across metrics.
    within = _rows({SETTLED_DAYS_AGO: int(BASE_AMOUNT * 1.1)})
    assert _run(dbt_project, test_id, within, max_change_percent=25) == "pass"

    # The same bucket moving well past the tolerance must still fail.
    beyond = _rows({SETTLED_DAYS_AGO: BASE_AMOUNT * 2})
    assert _run(dbt_project, test_id, beyond, max_change_percent=25) == "fail"
