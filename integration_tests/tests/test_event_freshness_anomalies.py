import random
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


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_exclude_detection_from_training(test_id: str, dbt_project: DbtProject):
    """
    Test the exclude_detection_period_from_training flag functionality for event freshness anomalies.

    Scenario:
    - 14 days total: 7 days normal (small jitter) + 7 days anomalous (large lag)
    - Without exclusion: 7 anomalous days contaminate training, test passes
    - With exclusion: only 7 normal days in training, anomaly detected, test fails
    """
    test_started_at = datetime.utcnow().replace(hour=0, minute=0, second=0)

    random.seed(42)
    normal_start = test_started_at - timedelta(days=14)
    normal_data = []
    for date in generate_dates(normal_start, step=STEP, days_back=7):
        jitter_minutes = random.randint(0, 10)
        normal_data.append(
            {
                EVENT_TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
                UPDATE_TIMESTAMP_COLUMN: (
                    date + timedelta(minutes=jitter_minutes)
                ).strftime(DATE_FORMAT),
            }
        )

    anomalous_start = test_started_at - timedelta(days=7)
    anomalous_data = []
    for date in generate_dates(anomalous_start, step=STEP, days_back=7):
        anomalous_data.append(
            {
                EVENT_TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
                UPDATE_TIMESTAMP_COLUMN: (date + timedelta(hours=5)).strftime(
                    DATE_FORMAT
                ),
            }
        )

    all_data = normal_data + anomalous_data

    test_args_without_exclusion = {
        "event_timestamp_column": EVENT_TIMESTAMP_COLUMN,
        "update_timestamp_column": UPDATE_TIMESTAMP_COLUMN,
        "days_back": 14,
        "backfill_days": 7,
        "time_bucket": {"period": "hour", "count": 1},
        "sensitivity": 3,
    }

    test_result_without_exclusion = dbt_project.test(
        test_id + "_without_exclusion",
        TEST_NAME,
        test_args_without_exclusion,
        data=all_data,
        test_vars={"custom_run_started_at": test_started_at.isoformat()},
    )

    assert (
        test_result_without_exclusion["status"] == "pass"
    ), "Test should pass when anomaly is included in training"

    test_args_with_exclusion = {
        **test_args_without_exclusion,
        "exclude_detection_period_from_training": True,
    }

    test_result_with_exclusion = dbt_project.test(
        test_id + "_with_exclusion",
        TEST_NAME,
        test_args_with_exclusion,
        data=all_data,
        test_vars={"custom_run_started_at": test_started_at.isoformat()},
    )

    assert (
        test_result_with_exclusion["status"] == "fail"
    ), "Test should fail when anomaly is excluded from training"
