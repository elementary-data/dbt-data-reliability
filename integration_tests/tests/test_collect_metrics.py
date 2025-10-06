from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pytest
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

METRICS_TABLE = "data_monitoring_metrics"
TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.collect_metrics"

COL_TO_METRIC_TYPES = {
    None: {"row_count"},
    "id": {"average"},
    "name": {"average_length"},
    "*": {"null_count"},
    ("id", "name"): {"zero_count"},  # Shouldn't do anything on 'name'.
}
EXPECTED_COL_TO_METRIC_NAMES = {
    None: {"custom_row_count"},
    "id": {"custom_average", "custom_null_count", "custom_zero_count"},
    "name": {"custom_average_length", "custom_null_count"},
    "updated_at": {"custom_null_count"},
}


DBT_TEST_ARGS = {
    "timestamp_column": TIMESTAMP_COLUMN,
    "metrics": [
        {"type": metric_type, "name": f"custom_{metric_type}", "columns": col_name}
        for col_name, metric_types in COL_TO_METRIC_TYPES.items()
        for metric_type in metric_types
    ],
}


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_collect_metrics(test_id: str, dbt_project: DbtProject):
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
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "pass"

    metrics = dbt_project.read_table(
        METRICS_TABLE,
        where=f"full_table_name LIKE '%{test_id.upper()}'",
    )
    col_to_metric_names = defaultdict(set)
    for metric in metrics:
        assert metric["metric_type"] is not None
        col_name = metric["column_name"].lower() if metric["column_name"] else None
        metric_name = metric["metric_name"]
        col_to_metric_names[col_name].add(metric_name)

    assert col_to_metric_names == EXPECTED_COL_TO_METRIC_NAMES


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_collect_no_timestamp_metrics(test_id: str, dbt_project: DbtProject):
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
    test_args = DBT_TEST_ARGS.copy()
    test_args.pop("timestamp_column")
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"

    metrics = dbt_project.read_table(
        METRICS_TABLE,
        where=f"full_table_name LIKE '%{test_id.upper()}'",
    )
    col_to_metric_names = defaultdict(set)
    for metric in metrics:
        col_name = metric["column_name"].lower() if metric["column_name"] else None
        metric_name = metric["metric_name"]
        col_to_metric_names[col_name].add(metric_name)

    assert col_to_metric_names == EXPECTED_COL_TO_METRIC_NAMES


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_collect_group_by_metrics(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "id": id,
            "name": name,
            "dimension": dim,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for id, name in [(1, "Superman"), (2, "Batman")]
        for dim in ["dim1", "dim2"]
    ]

    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        {
            **DBT_TEST_ARGS,
            "metrics": [
                metric
                for metric in DBT_TEST_ARGS["metrics"]
                if metric["type"] == "row_count"
            ],
            "dimensions": ["dimension"],
        },
        data=data,
    )

    assert test_result["status"] == "pass"

    expected_dim_to_col_to_metric_names = {
        "dim1": {None: {"custom_row_count"}},
        "dim2": {None: {"custom_row_count"}},
        None: {None: {"custom_row_count"}},
    }
    metrics = dbt_project.read_table(
        METRICS_TABLE,
        where=f"full_table_name LIKE '%{test_id.upper()}'",
    )
    dim_to_col_to_metric_names = defaultdict(lambda: defaultdict(set))
    for metric in metrics:
        dim_val = metric["dimension_value"]
        col_name = metric["column_name"].lower() if metric["column_name"] else None
        metric_name = metric["metric_name"]
        dim_to_col_to_metric_names[dim_val][col_name].add(metric_name)

    assert dim_to_col_to_metric_names == expected_dim_to_col_to_metric_names


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
@pytest.mark.skip_for_dbt_fusion
def test_collect_metrics_unique_metric_name(test_id: str, dbt_project: DbtProject):
    args = DBT_TEST_ARGS.copy()
    args["metrics"].append(args["metrics"][0])
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        args,
    )
    assert test_result["status"] == "error"

    args = DBT_TEST_ARGS.copy()
    args["metrics"][0].pop("name")
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        args,
    )
    assert test_result["status"] == "error"


def test_collect_metrics_elementary_disabled(test_id: str, dbt_project: DbtProject):
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
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        elementary_enabled=False,
    )
    assert test_result["status"] == "pass"
