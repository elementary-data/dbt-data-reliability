from datetime import datetime, timedelta
from dbt_project import DbtProject
from data_generator import generate_dates, DATE_FORMAT

TIMESTAMP_COLUMN = "timestamp"
VALUE_COLUMN = "value"
TEST_NAME = "elementary.column_anomalies"


def test_seasonality_day_of_week(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT), VALUE_COLUMN: i % 7}
        for i, date in enumerate(generate_dates(datetime.now().date()))
    ]
    result = dbt_project.test(
        data,
        test_id,
        TEST_NAME,
        dict(
            column_name=VALUE_COLUMN,
            timestamp_column=TIMESTAMP_COLUMN,
            column_anomalies=["max"],
            seasonality="day_of_week",
        ),
    )
    assert result["status"] == "pass"


def test_seasonality_day_of_week_full_drop(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT), VALUE_COLUMN: i % 7}
        for i, date in enumerate(
            generate_dates(datetime.now().date() - timedelta(days=1))
        )
    ]
    result = dbt_project.test(
        data,
        test_id,
        TEST_NAME,
        dict(
            column_name=VALUE_COLUMN,
            timestamp_column=TIMESTAMP_COLUMN,
            column_anomalies=["max"],
            seasonality="day_of_week",
        ),
    )
    assert result["status"] == "pass"


def test_seasonality_day_of_week_partial_drop(test_id: str, dbt_project: DbtProject):
    standard_value = 1000
    standard_day_of_week_value = 1000000
    starting_datetime = datetime.now() - timedelta(1)
    starting_day_of_week = starting_datetime.weekday()
    anomaly_data, *data = [
        {
            TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
            VALUE_COLUMN: standard_value
            if date.weekday() != starting_day_of_week
            else standard_day_of_week_value,
        }
        for i, date in enumerate(generate_dates(starting_datetime))
    ]
    anomaly_data[VALUE_COLUMN] = standard_value
    data.append(anomaly_data)
    result = dbt_project.test(
        data,
        test_id,
        TEST_NAME,
        dict(
            column_name=VALUE_COLUMN,
            timestamp_column=TIMESTAMP_COLUMN,
            column_anomalies=["max"],
            seasonality="day_of_week",
        ),
    )
    assert result["status"] == "fail"
