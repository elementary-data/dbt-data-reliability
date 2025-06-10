from datetime import datetime, timedelta

import pytest
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TEST_NAME = "elementary.event_freshness_anomalies"
EVENT_TIMESTAMP_COLUMN = "event_timestamp"
UPDATE_TIMESTAMP_COLUMN = "update_timestamp"
STEP = timedelta(hours=1)


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalyless_event_freshness(test_id: str, dbt_project: DbtProject):
    data = [
        {
            EVENT_TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
            UPDATE_TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
        }
        for date in generate_dates(datetime.now(), step=STEP)
    ]
    result = dbt_project.test(
        test_id,
        TEST_NAME,
        dict(
            event_timestamp_column=EVENT_TIMESTAMP_COLUMN,
            update_timestamp_column=UPDATE_TIMESTAMP_COLUMN,
        ),
        data=data,
    )
    assert result["status"] == "pass"


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_stop_event_freshness(test_id: str, dbt_project: DbtProject):
    anomaly_date = datetime.now() - timedelta(days=2)
    data = [
        {
            EVENT_TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
            UPDATE_TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
        }
        for date in generate_dates(anomaly_date, step=STEP)
    ]
    result = dbt_project.test(
        test_id,
        TEST_NAME,
        dict(
            event_timestamp_column=EVENT_TIMESTAMP_COLUMN,
            update_timestamp_column=UPDATE_TIMESTAMP_COLUMN,
        ),
        data=data,
    )
    assert result["status"] == "fail"


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_slower_rate_event_freshness(test_id: str, dbt_project: DbtProject):
    # To avoid races, set the "custom_started_at" to the beginning of the day
    test_started_at = datetime.utcnow().replace(hour=0, minute=0, second=0)

    anomaly_date = test_started_at - timedelta(days=1)
    data = [
        {
            EVENT_TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
            UPDATE_TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
        }
        for date in generate_dates(anomaly_date, step=STEP)
    ]
    slow_data = [
        {
            EVENT_TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
            UPDATE_TIMESTAMP_COLUMN: (date + STEP).strftime(DATE_FORMAT),
        }
        for date in generate_dates(test_started_at, step=STEP, days_back=1)
    ]
    data.extend(slow_data)
    result = dbt_project.test(
        test_id,
        TEST_NAME,
        dict(
            event_timestamp_column=EVENT_TIMESTAMP_COLUMN,
            update_timestamp_column=UPDATE_TIMESTAMP_COLUMN,
        ),
        data=data,
        test_vars={"custom_run_started_at": test_started_at.isoformat()},
    )
    assert result["status"] == "fail"
