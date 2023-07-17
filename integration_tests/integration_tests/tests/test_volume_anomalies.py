from datetime import datetime, timedelta

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.volume_anomalies"
DBT_TEST_ARGS = {"timestamp_column": TIMESTAMP_COLUMN}


def test_anomalyless_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now())
    ]
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert test_result["status"] == "pass"


def test_full_drop_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now())
        if date < datetime.now() - timedelta(days=3)
    ]
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert test_result["status"] == "fail"


def test_partial_drop_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now())
        for _ in range(2 if date < datetime.now() - timedelta(days=3) else 1)
    ]
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert test_result["status"] == "fail"


def test_spike_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now())
        for _ in range(1 if date < datetime.now() - timedelta(days=3) else 2)
    ]
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert test_result["status"] == "fail"
