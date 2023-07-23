from datetime import date, timedelta
from typing import Any, Dict, List

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
TEST_COLUMN = "superhero"
DBT_TEST_NAME = "elementary.column_anomalies"
DBT_TEST_ARGS = {
    "timestamp_column": TIMESTAMP_COLUMN,
    "column_anomalies": ["null_count"],
}


def test_anomalyless_column_anomalies(test_id: str, dbt_project: DbtProject):
    dates = generate_dates(base_date=date.today() - timedelta(1))
    data: List[Dict[str, Any]] = sum(
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
            for cur_date in dates
        ],
        [],
    )
    test_result = dbt_project.test(
        data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS, test_column=TEST_COLUMN
    )
    assert test_result["status"] == "pass"


def test_anomalous_column_anomalies(test_id: str, dbt_project: DbtProject):
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

    test_result = dbt_project.test(
        data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS, test_column=TEST_COLUMN
    )
    assert test_result["status"] == "fail"


def test_column_anomalies_with_where_expression(test_id: str, dbt_project: DbtProject):
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

    params = DBT_TEST_ARGS
    test_result = dbt_project.test(
        data, test_id, DBT_TEST_NAME, params, test_column=TEST_COLUMN
    )
    assert test_result["status"] == "fail"

    params = dict(DBT_TEST_ARGS, where="universe = 'Marvel'")
    test_result = dbt_project.test(
        data, test_id, DBT_TEST_NAME, params, test_column=TEST_COLUMN
    )
    assert test_result["status"] == "pass"

    params = dict(DBT_TEST_ARGS, where="universe = 'DC'")
    test_result = dbt_project.test(
        data, test_id, DBT_TEST_NAME, params, test_column=TEST_COLUMN
    )
    assert test_result["status"] == "fail"
