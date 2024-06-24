import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

METRICS_TABLE = "data_monitoring_metrics"
TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.collect_metrics"
DBT_TEST_ARGS = {
    "timestamp_column": TIMESTAMP_COLUMN,
}


def test_collect_numeric_column_metrics(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "id": id,
            "name": name,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for id, name in [(1, "Superman"), (2, "Batman")]
    ]
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, test_column="id"
    )
    assert test_result["status"] == "pass"

    expected_metrics = json.loads(
        dbt_project.dbt_runner.run_operation(
            "elementary.column_monitors_by_type", macro_args={"data_type": "numeric"}
        )[0]
    )

    metrics = dbt_project.read_table(
        METRICS_TABLE,
        where=f"full_table_name LIKE '%{test_id.upper()}' AND LOWER(column_name) = 'id'",
    )
    for metric_name in expected_metrics:
        assert any(
            row["metric_name"] == metric_name for row in metrics
        ), f"No metric found for name '{metric_name}'"


def test_collect_string_column_metrics(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "id": id,
            "name": name,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for id, name in [(1, "Superman"), (2, "Batman")]
    ]
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, test_column="name"
    )
    assert test_result["status"] == "pass"

    expected_metrics = json.loads(
        dbt_project.dbt_runner.run_operation(
            "elementary.column_monitors_by_type", macro_args={"data_type": "string"}
        )[0]
    )

    metrics = dbt_project.read_table(
        METRICS_TABLE,
        where=f"full_table_name LIKE '%{test_id.upper()}' AND LOWER(column_name) = 'name'",
    )
    for metric_name in expected_metrics:
        assert any(
            row["metric_name"] == metric_name for row in metrics
        ), f"No metric found for name '{metric_name}'"


def test_collect_table_metrics(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "name": name,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for name in ["Superman", "Batman"]
    ]
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "pass"

    expected_metrics = json.loads(
        dbt_project.dbt_runner.run_operation("elementary.get_default_table_monitors")[0]
    )
    metrics = dbt_project.read_table(
        METRICS_TABLE,
        where=f"full_table_name LIKE '%{test_id.upper()}'",
    )
    for metric_name in expected_metrics:
        assert any(
            row["metric_name"] == metric_name for row in metrics
        ), f"No metric found for name '{metric_name}'"


def test_collect_no_timestamp_column_metrics(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "name": name,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for name in ["Superman", "Batman"]
    ]
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, {}, data=data, test_column="name"
    )
    assert test_result["status"] == "pass"

    expected_metrics = json.loads(
        dbt_project.dbt_runner.run_operation(
            "elementary.column_monitors_by_type", macro_args={"data_type": "string"}
        )[0]
    )

    metrics = dbt_project.read_table(
        METRICS_TABLE,
        where=f"full_table_name LIKE '%{test_id.upper()}' AND LOWER(column_name) = 'name'",
    )
    for metric_name in expected_metrics:
        assert any(
            row["metric_name"] == metric_name for row in metrics
        ), f"No metric found for name '{metric_name}'"


def test_collect_no_timestamp_table_metrics(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "name": name,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for name in ["Superman", "Batman"]
    ]
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, {}, data=data)
    assert test_result["status"] == "pass"

    metrics = dbt_project.read_table(
        METRICS_TABLE,
        where=f"full_table_name LIKE '%{test_id.upper()}'",
    )
    assert any(
        row["metric_name"] == "row_count" for row in metrics
    ), "No metric found for name 'row_count'"
