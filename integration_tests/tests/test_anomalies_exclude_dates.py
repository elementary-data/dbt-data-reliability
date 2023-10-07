from datetime import datetime, timedelta
from typing import Any, Dict, List

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject
from parametrization import Parametrization

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.column_anomalies"
DBT_TEST_ARGS = {
    "timestamp_column": TIMESTAMP_COLUMN,
    "column_anomalies": ["sum"],
}


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="daily_buckets",
    time_bucket={"period": "day", "count": 1},
    dates_step=timedelta(days=1),
)
@Parametrization.case(
    name="six_hour_buckets",
    time_bucket={"period": "hour", "count": 6},
    dates_step=timedelta(hours=6),
)
def test_exclude_specific_dates(
    test_id: str, dbt_project: DbtProject, time_bucket: dict, dates_step: timedelta
):
    utc_now = datetime.utcnow()
    test_bucket, *training_buckets = generate_dates(
        base_date=utc_now - timedelta(1), step=dates_step
    )

    exclude_dates = [str(utc_now - timedelta(3)), str(utc_now - timedelta(5))]

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_bucket.strftime(DATE_FORMAT), "metric": 10}
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_bucket.strftime(DATE_FORMAT),
            "metric": 1 if str(cur_bucket.date()) not in exclude_dates else 10,
        }
        for cur_bucket in training_buckets
    ]

    test_args = {**DBT_TEST_ARGS, "time_bucket": time_bucket}
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="metric"
    )
    assert test_result["status"] == "pass"

    test_args = {
        **DBT_TEST_ARGS,
        "anomalies_exclude_dates": exclude_dates,
        "time_bucket": time_bucket,
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, test_column="metric"
    )
    assert test_result["status"] == "fail"


def test_exclude_range_with_after_and_before(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    after_date = utc_today - timedelta(6)
    before_date = utc_today - timedelta(3)
    exclude_dates = [{"after": str(after_date), "before": str(before_date)}]

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), "metric": 10}
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "metric": 1 if cur_date < after_date or cur_date >= before_date else 10,
        }
        for cur_date in training_dates
    ]

    test_args = {**DBT_TEST_ARGS, "days_back": 30}
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="metric"
    )
    assert test_result["status"] == "pass"

    test_args = {
        **DBT_TEST_ARGS,
        "anomalies_exclude_dates": exclude_dates,
        "days_back": 30,
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, test_column="metric"
    )
    assert test_result["status"] == "fail"


def test_exclude_range_with_before_only(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    before_date = utc_today - timedelta(14)
    exclude_dates = [{"before": str(before_date)}]

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), "metric": 10}
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "metric": 1 if cur_date >= before_date else 10,
        }
        for cur_date in training_dates
    ]

    test_args = {**DBT_TEST_ARGS, "days_back": 30}
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="metric"
    )
    assert test_result["status"] == "pass"

    test_args = {
        **DBT_TEST_ARGS,
        "anomalies_exclude_dates": exclude_dates,
        "days_back": 30,
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, test_column="metric"
    )
    assert test_result["status"] == "fail"


def test_invalid_configurations(test_id: str, dbt_project: DbtProject):
    test_args = {**DBT_TEST_ARGS, "anomalies_exclude_dates": 123}
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=[], test_column="metric"
    )
    assert test_result["status"] == "error"

    test_args = {**DBT_TEST_ARGS, "anomalies_exclude_dates": {"pasten": 5}}
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, test_column="metric"
    )
    assert test_result["status"] == "error"

    test_args = {**DBT_TEST_ARGS, "anomalies_exclude_dates": {"before": "not_a_date"}}
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, test_column="metric"
    )
    assert test_result["status"] == "error"
