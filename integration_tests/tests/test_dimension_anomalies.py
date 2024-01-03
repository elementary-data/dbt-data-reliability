from datetime import datetime, timedelta
from typing import Any, Dict, List

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.dimension_anomalies"
DBT_TEST_ARGS = {"timestamp_column": TIMESTAMP_COLUMN, "dimensions": ["superhero"]}


def test_anomalyless_dimension_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Spiderman"]
    ]
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "pass"


def test_anomalous_dimension_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for superhero in ["Superman", "Superman", "Superman", "Spiderman"]
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in training_dates
        for superhero in ["Superman", "Spiderman"]
    ]

    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "fail"


def test_dimensions_anomalies_with_where_parameter(
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
            ("DC", "Superman"),
            ("DC", "Superman"),
            ("DC", "Superman"),
            ("Marvel", "Spiderman"),
        ]
    ] + [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "universe": universe,
            "superhero": superhero,
        }
        for cur_date in training_dates
        for universe, superhero in [("DC", "Superman"), ("Marvel", "Spiderman")]
    ]

    params = DBT_TEST_ARGS
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, params, data=data)
    assert test_result["status"] == "fail"

    params = dict(DBT_TEST_ARGS, where="universe = 'Marvel'")
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, params, test_vars={"force_metrics_backfill": True}
    )
    assert test_result["status"] == "pass"

    params = dict(params, where="universe = 'DC'")
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, params, test_vars={"force_metrics_backfill": True}
    )
    assert test_result["status"] == "fail"
