from datetime import date, timedelta
from typing import Any, Dict, List

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.dimension_anomalies"
DBT_TEST_ARGS = {"timestamp_column": TIMESTAMP_COLUMN, "dimensions": ["superhero"]}


def test_anomalyless_dimension_anomalies(test_id: str, dbt_project: DbtProject):
    dates = generate_dates(base_date=date.today() - timedelta(1))
    data: List[Dict[str, Any]] = sum(
        [
            [
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "superhero": "Superman",
                },
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "superhero": "Batman",
                },
            ]
            for cur_date in dates
        ],
        [],
    )
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert test_result["status"] == "pass"


def test_anomalous_dimension_anomalies(test_id: str, dbt_project: DbtProject):
    test_date, *training_dates = generate_dates(base_date=date.today() - timedelta(1))
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: test_date,
            "superhero": "Superman",
        },
        {
            TIMESTAMP_COLUMN: test_date,
            "superhero": "Superman",
        },
        {
            TIMESTAMP_COLUMN: test_date,
            "superhero": "Superman",
        },
        {
            TIMESTAMP_COLUMN: test_date,
            "superhero": "Batman",
        },
    ] + sum(
        [
            [
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "superhero": "Superman",
                },
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "superhero": "Batman",
                },
            ]
            for cur_date in training_dates
        ],
        [],
    )
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert test_result["status"] == "fail"


def test_dimensions_anomalies_with_where_parameter(
    test_id: str, dbt_project: DbtProject
):
    test_date, *training_dates = generate_dates(base_date=date.today() - timedelta(1))
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: test_date,
            "universe": "DC",
            "superhero": "Superman",
        },
        {
            TIMESTAMP_COLUMN: test_date,
            "universe": "DC",
            "superhero": "Superman",
        },
        {
            TIMESTAMP_COLUMN: test_date,
            "universe": "DC",
            "superhero": "Superman",
        },
        {
            TIMESTAMP_COLUMN: test_date,
            "universe": "Marvel",
            "superhero": "Spiderman",
        },
    ] + sum(
        [
            [
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "universe": "DC",
                    "superhero": "Superman",
                },
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "universe": "Marvel",
                    "superhero": "Spiderman",
                },
            ]
            for cur_date in training_dates
        ],
        [],
    )

    params = DBT_TEST_ARGS
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, params)
    assert test_result["status"] == "fail"

    params = dict(DBT_TEST_ARGS, where="universe = 'Marvel'")
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, params)
    assert test_result["status"] == "pass"

    params = dict(params, where="universe = 'DC'")
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, params)
    assert test_result["status"] == "fail"
