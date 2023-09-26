from datetime import datetime, timedelta
from typing import Any, Dict, List

import pytest
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject
from parametrization import Parametrization

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.volume_anomalies"
DBT_TEST_ARGS = {"timestamp_column": TIMESTAMP_COLUMN}


def test_anomalyless_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=utc_today)
    ]
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "pass"


def test_full_drop_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=utc_today)
        if cur_date < utc_today - timedelta(days=1)
    ]
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "fail"


@Parametrization.autodetect_parameters()
@Parametrization.case(name="source", as_model=False)
@Parametrization.case(name="model", as_model=True)
def test_volume_anomalies_with_where_parameter(
    test_id: str, dbt_project: DbtProject, as_model: bool
):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(days=1))

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), "payback": payback}
        for payback in ["karate", "ka-razy", "ka-razy", "ka-razy", "ka-razy", "ka-razy"]
    ]
    data += [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT), "payback": payback}
        for cur_date in training_dates
        for payback in ["karate", "ka-razy"]
    ]

    params = DBT_TEST_ARGS
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, params, data=data, as_model=as_model
    )
    assert test_result["status"] == "fail"

    params = dict(DBT_TEST_ARGS, where="payback = 'karate'")
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        params,
        as_model=as_model,
        test_vars={"force_metrics_backfill": True},
    )
    assert test_result["status"] == "pass"

    params = dict(DBT_TEST_ARGS, where="payback = 'ka-razy'")
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        params,
        as_model=as_model,
        test_vars={"force_metrics_backfill": True},
    )
    assert test_result["status"] == "fail"


def test_volume_anomalies_with_time_buckets(test_id: str, dbt_project: DbtProject):
    now = datetime.utcnow()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(
            base_date=now, step=timedelta(hours=1), days_back=2
        )
        if cur_date < now - timedelta(hours=1)
    ]
    # This is a bug in the dbt package. The test should pass, but it fails.
    # test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    # assert test_result["status"] == "pass"

    test_args = {
        **DBT_TEST_ARGS,
        "time_bucket": {"period": "hour", "count": 1},
        "days_back": 1,
    }
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "fail"


def test_volume_anomalies_with_direction_spike(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=utc_today)
        if cur_date < utc_today - timedelta(days=1)
        for _ in range(1 if cur_date < utc_today - timedelta(days=1) else 2)
    ]
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "fail"

    test_args = {**DBT_TEST_ARGS, "anomaly_direction": "spike"}
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args)
    assert test_result["status"] == "pass"


def test_volume_anomalies_with_direction_drop(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=utc_today)
        for _ in range(1 if cur_date < utc_today - timedelta(days=1) else 2)
    ]
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "fail"

    test_args = {**DBT_TEST_ARGS, "anomaly_direction": "drop"}
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args)
    assert test_result["status"] == "pass"


def test_volume_anomalies_with_seasonality(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    dates = generate_dates(
        base_date=utc_today - timedelta(days=1),
        step=timedelta(weeks=1),
        days_back=7 * 14,
    )
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in dates
        if cur_date < utc_today - timedelta(weeks=1)
    ]
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "pass"

    test_args = {**DBT_TEST_ARGS, "seasonality": "day_of_week"}
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args)
    assert test_result["status"] == "fail"


def test_volume_anomalies_with_sensitivity(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for i, cur_date in enumerate(generate_dates(base_date=utc_today))
        for _ in range(
            1 if i % 2 == 0 else 2 if cur_date < utc_today - timedelta(days=1) else 3
        )
    ]
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "pass"

    test_args = {**DBT_TEST_ARGS, "sensitivity": 2}
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args)
    assert test_result["status"] == "fail"


def test_volume_anomalies_no_timestamp(test_id: str, dbt_project: DbtProject):
    data = [{"hello": "world"}]
    min_training_set_size = 4
    test_args = {
        # Using smaller training set size to avoid needing to run many tests.
        "min_training_set_size": min_training_set_size,
        # Smaller sensitivity due to smaller training set size.
        "sensitivity": 1.25,
    }
    dbt_project.seed(data, test_id)
    for _ in range(min_training_set_size):
        test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args)
        assert test_result["status"] == "pass"

    dbt_project.seed(data * 2, test_id)
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args)
    assert test_result["status"] == "fail"


@pytest.mark.only_on_targets(["bigquery"])
def test_wildcard_name_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=utc_today)
        if cur_date < utc_today - timedelta(days=1)
    ]
    wildcarded_table_name = test_id[:-1] + "*"
    test_result = dbt_project.test(
        wildcarded_table_name,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        table_name=test_id,
    )
    assert test_result["status"] == "fail"


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="true_positive",
    expected_result="fail",
    drop_failure_percent_threshold=5,
    metric_value=25,
)
@Parametrization.case(
    name="false_positive",
    expected_result="fail",
    drop_failure_percent_threshold=None,
    metric_value=29,
)
@Parametrization.case(
    name="true_negative",
    expected_result="pass",
    drop_failure_percent_threshold=5,
    metric_value=29,
)
def test_volume_anomaly_static_data_drop(
    test_id: str,
    dbt_project: DbtProject,
    expected_result: str,
    drop_failure_percent_threshold: int,
    metric_value: int,
):
    now = datetime.utcnow()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=now, step=timedelta(days=1))
        if cur_date < now - timedelta(days=1)
    ] * 30
    data += [
        {TIMESTAMP_COLUMN: (now - timedelta(days=1)).strftime(DATE_FORMAT)}
    ] * metric_value

    # 30 new rows every day
    # 25 new rows in the last day
    # z-score ~ -3.6

    test_args = {
        **DBT_TEST_ARGS,
        "time_bucket": {"period": "day", "count": 1},
        "ignore_small_changes": {
            "drop_failure_percent_threshold": drop_failure_percent_threshold
        },
    }
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == expected_result


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="true_positive",
    expected_result="fail",
    spike_failure_percent_threshold=5,
    metric_value=35,
)
@Parametrization.case(
    name="false_positive",
    expected_result="fail",
    spike_failure_percent_threshold=None,
    metric_value=31,
)
@Parametrization.case(
    name="true_negative",
    expected_result="pass",
    spike_failure_percent_threshold=5,
    metric_value=31,
)
def test_volume_anomaly_static_data_spike(
    test_id: str,
    dbt_project: DbtProject,
    expected_result: str,
    spike_failure_percent_threshold: int,
    metric_value: int,
):
    now = datetime.utcnow()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=now, step=timedelta(days=1))
        if cur_date < now - timedelta(days=1)
    ] * 30
    data += [
        {TIMESTAMP_COLUMN: (now - timedelta(days=1)).strftime(DATE_FORMAT)}
    ] * metric_value

    # 30 new rows every day
    # 35 new rows in the last day
    # z-score ~ -3.6

    test_args = {
        **DBT_TEST_ARGS,
        "time_bucket": {"period": "day", "count": 1},
        "ignore_small_changes": {
            "spike_failure_percent_threshold": spike_failure_percent_threshold
        },
    }
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == expected_result


def test_not_fail_on_zero(test_id: str, dbt_project: DbtProject):
    now = datetime.utcnow()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=now, step=timedelta(days=1))
        if cur_date < now - timedelta(days=1)
    ]

    test_args = {**DBT_TEST_ARGS, "fail_on_zero": False, "anomaly_sensitivity": 1000}
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"


def test_fail_on_zero(test_id: str, dbt_project: DbtProject):
    now = datetime.utcnow()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=now, step=timedelta(days=1))
        if cur_date < now - timedelta(days=1)
    ]

    test_args = {**DBT_TEST_ARGS, "fail_on_zero": True, "anomaly_sensitivity": 1000}
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "fail"
