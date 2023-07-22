from datetime import date, timedelta
from typing import Any, Dict, List

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
TEST_COLUMN = "superhero"
DBT_TEST_NAME = "elementary.all_columns_anomalies"
DBT_TEST_ARGS = {
    "timestamp_column": TIMESTAMP_COLUMN,
    "column_anomalies": ["null_count"],
}


def test_anomalyless_all_columns_anomalies(test_id: str, dbt_project: DbtProject):
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
                {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT), "superhero": None},
            ]
            for cur_date in dates
        ],
        [],
    )
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert test_result["status"] == "pass"


def test_anomalous_all_columns_anomalies(test_id: str, dbt_project: DbtProject):
    test_date, *training_dates = generate_dates(base_date=date.today() - timedelta(1))
    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), TEST_COLUMN: None},
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), TEST_COLUMN: None},
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), TEST_COLUMN: None},
    ] + sum(
        [
            [
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    TEST_COLUMN: "Superman",
                },
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    TEST_COLUMN: "Batman",
                },
                {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT), TEST_COLUMN: None},
            ]
            for cur_date in training_dates
        ],
        [],
    )

    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert test_result["status"] == "fail"


def test_all_columns_anomalies_with_where_expression(
    test_id: str, dbt_project: DbtProject
):
    test_date, *training_dates = generate_dates(base_date=date.today() - timedelta(1))

    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "universe": "DC",
            TEST_COLUMN: None,
        },
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "universe": "DC",
            TEST_COLUMN: None,
        },
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "universe": "DC",
            TEST_COLUMN: None,
        },
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "universe": "Marvel",
            TEST_COLUMN: "Spiderman",
        },
    ] + sum(
        [
            [
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "universe": "DC",
                    TEST_COLUMN: "Superman",
                },
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "universe": "DC",
                    TEST_COLUMN: "Batman",
                },
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "universe": "DC",
                    TEST_COLUMN: None,
                },
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "universe": "Marvel",
                    TEST_COLUMN: "Spiderman",
                },
            ]
            for cur_date in training_dates
        ],
        [],
    )

    params_without_where = DBT_TEST_ARGS
    result_without_where = dbt_project.test(
        data, test_id, DBT_TEST_NAME, params_without_where
    )
    assert result_without_where["status"] == "fail"

    params_with_where = dict(params_without_where, where="universe = 'Marvel'")
    result_with_where = dbt_project.test(
        data, test_id, DBT_TEST_NAME, params_with_where
    )
    assert result_with_where["status"] == "pass"

    params_with_where2 = dict(params_without_where, where="universe = 'DC'")
    result_with_where2 = dbt_project.test(
        data, test_id, DBT_TEST_NAME, params_with_where2
    )
    assert result_with_where2["status"] == "fail"
