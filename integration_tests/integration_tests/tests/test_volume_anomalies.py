from datetime import datetime, timedelta

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.volume_anomalies"
DBT_TEST_ARGS = {"timestamp_column": TIMESTAMP_COLUMN}


def read_status(test_id: str, dbt_project: DbtProject):
    return dbt_project.read_table(
        "elementary_test_results",
        where=f"table_name = '{test_id}'",
        column_names=["status"],
    )


def test_anomalyless_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now())
        for _ in range(10)
    ]
    dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert all(
        result["status"] == "pass" for result in read_status(test_id, dbt_project)
    )


def test_full_drop_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now())
        for _ in range(10)
        if date < datetime.now() - timedelta(days=2)
    ]
    dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert all(
        result["status"] == "fail" for result in read_status(test_id, dbt_project)
    )


def test_partial_drop_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now())
        for _ in range(10 if date < datetime.now() - timedelta(days=2) else 1)
    ]
    dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert all(
        result["status"] == "fail" for result in read_status(test_id, dbt_project)
    )


def test_spike_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now())
        for _ in range(10 if date < datetime.now() - timedelta(days=2) else 100)
    ]
    dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert all(
        result["status"] == "fail" for result in read_status(test_id, dbt_project)
    )
