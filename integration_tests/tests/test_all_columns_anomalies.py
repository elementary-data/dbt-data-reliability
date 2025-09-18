from datetime import datetime, timedelta
from typing import Any, Dict, List

import pytest
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.all_columns_anomalies"
DBT_TEST_ARGS = {
    "timestamp_column": TIMESTAMP_COLUMN,
    "column_anomalies": ["null_count"],
}


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalyless_all_columns_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Batman"]
    ]
    test_results = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, multiple_results=True
    )
    assert all([res["status"] == "pass" for res in test_results])


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalous_all_columns_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), "superhero": None}
        for _ in range(3)
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in training_dates
        for superhero in ["Superman", "Batman"]
    ]

    test_results = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, multiple_results=True
    )
    col_to_status = {res["column_name"].lower(): res["status"] for res in test_results}
    assert col_to_status == {"superhero": "fail", TIMESTAMP_COLUMN: "pass"}


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_all_columns_anomalies_with_where_parameter(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "universe": universe,
            "superhero": superhero,
        }
        for universe, superhero in [
            ("DC", None),
            ("DC", None),
            ("DC", None),
            ("Marvel", "Spiderman"),
        ]
    ] + [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "universe": universe,
            "superhero": superhero,
        }
        for cur_date in training_dates
        for universe, superhero in [
            ("DC", "Superman"),
            ("DC", "Batman"),
            ("DC", None),
            ("Marvel", "Spiderman"),
        ]
    ]

    test_results = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, multiple_results=True
    )
    col_to_status = {res["column_name"].lower(): res["status"] for res in test_results}
    assert col_to_status == {
        "superhero": "fail",
        TIMESTAMP_COLUMN: "pass",
        "universe": "pass",
    }

    test_results = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        multiple_results=True,
        test_vars={"force_metrics_backfill": True},
        test_config={"where": "universe = 'Marvel'"},
    )
    assert all([res["status"] == "pass" for res in test_results])

    test_results = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        multiple_results=True,
        test_vars={"force_metrics_backfill": True},
        test_config={"where": "universe = 'DC'"},
    )
    col_to_status = {res["column_name"].lower(): res["status"] for res in test_results}
    assert col_to_status == {
        "superhero": "fail",
        TIMESTAMP_COLUMN: "pass",
        "universe": "pass",
    }


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalyless_all_columns_anomalies_all_monitors_sanity(
    test_id: str, dbt_project: DbtProject
):
    # Tests above run on a specific monitor (null count) since it's easier to test accurately.
    # Nonetheless, it's useful to actually do a sanity on the full monitors list since it can detect things such as syntax errors.
    # Down the line it may be good to actually have a test per column monitor.

    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,  # String column
            "flights": 5,  # Numeric column
            "has_flown": True,  # Boolean column
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Batman"]
    ]
    test_args = {"timestamp_column": TIMESTAMP_COLUMN}
    test_results = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, multiple_results=True
    )
    assert all([res["status"] == "pass" for res in test_results])
