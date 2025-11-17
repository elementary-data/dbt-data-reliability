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
    - 7 days of normal data (5 minute lag between event and update) - training period
    - 7 days of anomalous data (5 hour lag) - detection period
    - Without exclusion: anomaly gets included in training baseline, test passes (misses anomaly)
    - With exclusion: anomaly excluded from training, test fails (detects anomaly)

    """
    utc_now = datetime.utcnow()
    test_started_at = (utc_now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Generate 7 days of normal data with varying lag (2-8 minutes) to ensure training_stddev > 0
    training_lags_minutes = [2, 3, 4, 5, 6, 7, 8]
    normal_data = []
    for i in range(7):
        event_date = test_started_at - timedelta(days=14 - i)
        event_time = event_date.replace(hour=12, minute=0, second=0, microsecond=0)
        update_time = event_time + timedelta(minutes=training_lags_minutes[i])
        normal_data.append(
            {
                EVENT_TIMESTAMP_COLUMN: event_time.strftime(DATE_FORMAT),
                UPDATE_TIMESTAMP_COLUMN: update_time.strftime(DATE_FORMAT),
            }
        )

    # Generate 7 days of anomalous data with 5-hour lag (detection period)
    anomalous_data = []
    for i in range(7):
        event_date = test_started_at - timedelta(days=7 - i)
        event_time = event_date.replace(hour=12, minute=0, second=0, microsecond=0)
        update_time = event_time + timedelta(hours=5)
        anomalous_data.append(
            {
                EVENT_TIMESTAMP_COLUMN: event_time.strftime(DATE_FORMAT),
                UPDATE_TIMESTAMP_COLUMN: update_time.strftime(DATE_FORMAT),
            }
        )

    all_data = normal_data + anomalous_data

    # Test 1: WITHOUT exclusion (should pass - misses the anomaly because it's included in training)
    test_args_without_exclusion = {
        "event_timestamp_column": EVENT_TIMESTAMP_COLUMN,
        "update_timestamp_column": UPDATE_TIMESTAMP_COLUMN,
        "days_back": 14,  # Scoring window: 14 days to include both training and detection
        "backfill_days": 7,  # Detection period: last 7 days (days 7-1 before test_started_at)
        "time_bucket": {
            "period": "day",
            "count": 1,
        },  # Daily buckets to avoid boundary issues
        "sensitivity": 3,
        "anomaly_direction": "spike",  # Explicit direction since we're testing increased lag
        "min_training_set_size": 5,  # Explicit minimum to avoid threshold issues
        # exclude_detection_period_from_training is not set (defaults to False/None)
    }

    test_result_without_exclusion = dbt_project.test(
        test_id + "_without_exclusion",
        TEST_NAME,
        test_args_without_exclusion,
        data=all_data,
        test_vars={
            "custom_run_started_at": test_started_at.isoformat(),
            "force_metrics_backfill": True,
        },
    )

    # This should PASS because the anomaly is included in training, making it part of the baseline
    assert (
        test_result_without_exclusion["status"] == "pass"
    ), "Test should pass when anomaly is included in training"

    # Test 2: WITH exclusion (should fail - detects the anomaly because it's excluded from training)
    test_args_with_exclusion = {
        **test_args_without_exclusion,
        "exclude_detection_period_from_training": True,
    }

    test_result_with_exclusion = dbt_project.test(
        test_id + "_with_exclusion",
        TEST_NAME,
        test_args_with_exclusion,
        data=all_data,
        test_vars={
            "custom_run_started_at": test_started_at.isoformat(),
            "force_metrics_backfill": True,
        },
    )

    # This should FAIL because the anomaly is excluded from training, so it's detected as anomalous
    assert (
        test_result_with_exclusion["status"] == "fail"
    ), "Test should fail when anomaly is excluded from training"
