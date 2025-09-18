from datetime import datetime, timedelta
from typing import Any, Dict, List

import pytest
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
# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_exclude_specific_dates(
    test_id: str, dbt_project: DbtProject, time_bucket: dict, dates_step: timedelta
):
    utc_now = datetime.utcnow()
    test_bucket, *training_buckets = generate_dates(
        base_date=utc_now - timedelta(1), step=dates_step
    )

    exclude_dates = [
        (utc_now - timedelta(5)).date(),
        (utc_now - timedelta(3)).date(),
    ]

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_bucket.strftime(DATE_FORMAT), "metric": 10}
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_bucket.strftime(DATE_FORMAT),
            "metric": 1 if cur_bucket.date() not in exclude_dates else 10,
        }
        for cur_bucket in training_buckets
    ]

    test_args = {**DBT_TEST_ARGS, "time_bucket": time_bucket}
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="metric"
    )
    assert test_result["status"] == "pass"

    excluded_dates_str = ", ".join(
        [f"cast('{cur_date}' as date)" for cur_date in exclude_dates]
    )
    test_args = {
        **DBT_TEST_ARGS,
        "anomaly_exclude_metrics": f"metric_date in ({excluded_dates_str})",
        "time_bucket": time_bucket,
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, test_column="metric"
    )
    assert test_result["status"] == "fail"


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_exclude_specific_timestamps(test_id: str, dbt_project: DbtProject):
    # To avoid races, set the "custom_started_at" to the beginning of the hour
    test_started_at = datetime.utcnow().replace(minute=0, second=0)

    test_bucket, *training_buckets = generate_dates(
        base_date=test_started_at - timedelta(hours=1),
        step=timedelta(hours=1),
        days_back=1,
    )

    excluded_buckets = [
        test_started_at - timedelta(hours=22),
        test_started_at - timedelta(hours=20),
    ]

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_bucket.strftime(DATE_FORMAT), "metric": 10}
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_bucket.strftime(DATE_FORMAT),
            "metric": 1 if cur_bucket not in excluded_buckets else 10,
        }
        for cur_bucket in training_buckets
    ]

    time_bucket = {"period": "hour", "count": 1}
    test_args = {**DBT_TEST_ARGS, "time_bucket": time_bucket, "days_back": 1}
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="metric"
    )
    assert test_result["status"] == "pass"

    excluded_buckets_str = ", ".join(
        [
            "cast('%s' as timestamp)" % cur_ts.strftime(DATE_FORMAT)
            for cur_ts in excluded_buckets
        ]
    )
    test_args = {
        **DBT_TEST_ARGS,
        "time_bucket": time_bucket,
        "days_back": 1,
        "anomaly_exclude_metrics": f"metric_time_bucket in ({excluded_buckets_str})",
    }
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args,
        test_column="metric",
        test_vars={"custom_run_started_at": test_started_at.isoformat()},
    )
    assert test_result["status"] == "fail"


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_exclude_date_range(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    start_date = utc_today - timedelta(6)
    end_date = utc_today - timedelta(3)

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), "metric": 10}
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "metric": 1 if cur_date < start_date or cur_date > end_date else 10,
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
        "anomaly_exclude_metrics": f"metric_date >= cast('{start_date}' as date) and metric_date <= cast('{end_date}' as date)",
        "days_back": 30,
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, test_column="metric"
    )
    assert test_result["status"] == "fail"


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_exclude_by_metric_value(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), "metric": 10}
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "metric": 1 if cur_date.day % 3 > 0 else 10,
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
        "anomaly_exclude_metrics": f"metric_date < cast('{test_date}' as date) and metric_value >= 5",
        "days_back": 30,
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, test_column="metric"
    )
    assert test_result["status"] == "fail"
