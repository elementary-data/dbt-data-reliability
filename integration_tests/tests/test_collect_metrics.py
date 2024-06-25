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


def test_collect_group_by_metrics(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
            "dimension": dim,
        }
        for cur_date in training_dates
        for superhero in ["Superman", "Batman"]
        for dim in ["dim1", "dim2"]
    ]

    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "superhero": None,
            "dimension": "dim1",
        }
        for _ in range(100)
    ]

    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        {**DBT_TEST_ARGS, "dimensions": ["dimension"]},
        data=data,
        test_column="superhero",
    )

    assert test_result["status"] == "pass"

    metrics = {
        row["dimension_value"]: row["sum_metric_value"]
        for row in dbt_project.read_table(
            METRICS_TABLE,
            where="metric_name = 'null_count'",
            group_by="dimension_value, metric_name",
            column_names=[
                "dimension_value",
                "metric_name",
                "sum(metric_value) as sum_metric_value",
            ],
        )
    }
    assert metrics["dim1"] == 100
    assert metrics["dim2"] == 0
