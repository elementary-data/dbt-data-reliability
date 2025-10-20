from datetime import datetime, timedelta

import pytest
from data_generator import DATE_FORMAT
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.volume_anomalies"
DBT_TEST_ARGS = {"timestamp_column": TIMESTAMP_COLUMN}


@pytest.mark.skip_targets(["clickhouse"])
def test_exclude_detection_period_from_training_baseline(
    test_id: str, dbt_project: DbtProject
):
    """
    Test case for CORE-19: Validates the exclude_detection_period_from_training flag functionality.

    This test demonstrates the core use case where:
    1. Detection period contains anomalous data that gets absorbed into training baseline
    2. WITHOUT exclusion: Anomaly is missed (test passes) because it's included in training
    3. WITH exclusion: Anomaly is detected (test fails) because it's excluded from training

    Test Scenario:
    - 30 days of normal data: 100 rows per day (baseline pattern)
    - 7 days of anomalous data: 110 rows per day (10% increase) in the detection period
    - Training period: 30 days
    - Detection period: 7 days
    - Time bucket: Daily aggregation
    - Sensitivity: 10 (high threshold to demonstrate masking effect)

    The 10% increase across 7 days gets absorbed into the cumulative training average,
    making the anomaly undetectable with the current implementation.

    Current Behavior (WITHOUT flag):
    - Test PASSES (no anomaly detected) because the 10% increase is absorbed into the
      cumulative training baseline when detection period data is included.

    Expected Behavior (WITH flag):
    - Test FAILS (anomaly detected) because the detection period is excluded from training,
      so the 10% increase is properly detected against the clean 30-day baseline.
    """
    now = datetime.utcnow()

    normal_data = []
    for day_offset in range(37, 7, -1):
        date = now - timedelta(days=day_offset)
        for _ in range(100):
            normal_data.append({TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)})

    anomalous_data = []
    for day_offset in range(7, 0, -1):
        date = now - timedelta(days=day_offset)
        for _ in range(110):
            anomalous_data.append({TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)})

    data = normal_data + anomalous_data

    test_args = {
        **DBT_TEST_ARGS,
        "time_bucket": {"period": "day", "count": 1},
        "training_period": {"period": "day", "count": 30},
        "detection_period": {"period": "day", "count": 7},
        "sensitivity": 10,
    }

    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args,
        data=data,
    )

    # Current behavior: Test PASSES (no anomaly detected)
    # The 10% increase is absorbed into the cumulative training baseline
    assert test_result["status"] == "pass", (
        "Test should PASS in current implementation (without exclusion flag). "
        "The 10% increase is absorbed into training, masking the anomaly."
    )

    # TODO: When the exclude_detection_period_from_training flag is implemented, (important-comment)
    # add a second test here that sets the flag to True and expects FAIL: (important-comment)
    # test_args_with_exclusion = {
    #     **test_args,
    #     "exclude_detection_period_from_training": True, (important-comment)
    # }
    # test_result_with_exclusion = dbt_project.test( (important-comment)
    #     test_id,
    #     DBT_TEST_NAME,
    #     test_args_with_exclusion,
    #     test_vars={"force_metrics_backfill": True}, (important-comment)
    # )
    # assert test_result_with_exclusion["status"] == "fail", ( (important-comment)
    #     "Test should FAIL with exclusion flag enabled. " (important-comment)
    #     "The 10% increase is detected against the clean baseline."
    # )
