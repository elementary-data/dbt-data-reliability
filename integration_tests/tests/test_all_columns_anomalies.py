from datetime import datetime, timedelta
from typing import Any, Dict, List

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.all_columns_anomalies"
DBT_TEST_ARGS = {
    "timestamp_column": TIMESTAMP_COLUMN,
    "column_anomalies": ["null_count"],
}


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


def test_all_columns_anomalies_with_where_expression(
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

    params = DBT_TEST_ARGS
    test_results = dbt_project.test(
        test_id, DBT_TEST_NAME, params, data=data, multiple_results=True
    )
    col_to_status = {res["column_name"].lower(): res["status"] for res in test_results}
    assert col_to_status == {
        "superhero": "fail",
        TIMESTAMP_COLUMN: "pass",
        "universe": "pass",
    }

    params = dict(DBT_TEST_ARGS, where="universe = 'Marvel'")
    test_results = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        params,
        multiple_results=True,
        test_vars={"force_metrics_backfill": True},
    )
    assert all([res["status"] == "pass" for res in test_results])

    params = dict(DBT_TEST_ARGS, where="universe = 'DC'")
    test_results = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        params,
        multiple_results=True,
        test_vars={"force_metrics_backfill": True},
    )
    col_to_status = {res["column_name"].lower(): res["status"] for res in test_results}
    assert col_to_status == {
        "superhero": "fail",
        TIMESTAMP_COLUMN: "pass",
        "universe": "pass",
    }
