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
def test_exclude_detection_period_from_training_event_freshness(
    test_id: str, dbt_project: DbtProject
):
    """
    Test the exclude_detection_period_from_training flag for event freshness anomalies.

    Scenario:
    - 30 days of normal data (event lag of 1 hour)
    - 7 days of anomalous data (event lag of 1.1 hours - 10% slower) in detection period
    - Without exclusion: anomaly gets included in training baseline, test passes (misses anomaly)
    - With exclusion: anomaly excluded from training, test fails (detects anomaly)
    """
    now = datetime.now()

    normal_data = [
        {
            EVENT_TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
            UPDATE_TIMESTAMP_COLUMN: (date + timedelta(hours=1)).strftime(DATE_FORMAT),
        }
        for date in generate_dates(
            base_date=now - timedelta(days=37), step=timedelta(hours=2), days_back=30
        )
    ]

    # Generate 7 days of anomalous data (event lag of 1.1 hours - 10% slower)
    anomalous_data = [
        {
            EVENT_TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
            UPDATE_TIMESTAMP_COLUMN: (date + timedelta(hours=1, minutes=6)).strftime(
                DATE_FORMAT
            ),
        }
        for date in generate_dates(
            base_date=now - timedelta(days=7), step=timedelta(hours=2), days_back=7
        )
    ]

    all_data = normal_data + anomalous_data

    # Test 1: WITHOUT exclusion (should pass - misses the anomaly)
    test_args_without_exclusion = {
        "event_timestamp_column": EVENT_TIMESTAMP_COLUMN,
        "update_timestamp_column": UPDATE_TIMESTAMP_COLUMN,
        "training_period": {"period": "day", "count": 30},
        "detection_period": {"period": "day", "count": 7},
        "time_bucket": {"period": "day", "count": 1},
        "sensitivity": 10,
    }

    test_result_without_exclusion = dbt_project.test(
        test_id + "_without_exclusion",
        TEST_NAME,
        test_args_without_exclusion,
        data=all_data,
    )

    assert (
        test_result_without_exclusion["status"] == "pass"
    ), "Test should pass when anomaly is included in training"

    # Test 2: WITH exclusion (should fail - detects the anomaly)
    test_args_with_exclusion = {
        **test_args_without_exclusion,
        "exclude_detection_period_from_training": True,
    }

    test_result_with_exclusion = dbt_project.test(
        test_id + "_with_exclusion",
        TEST_NAME,
        test_args_with_exclusion,
        data=all_data,
    )

    assert (
        test_result_with_exclusion["status"] == "fail"
    ), "Test should fail when anomaly is excluded from training"
