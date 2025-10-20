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
    Test case for CORE-19: Demonstrates current behavior with detection period in training.

    This test shows how the current implementation handles anomalous data in the detection period.
    The cumulative window function in get_anomaly_scores_query.sql includes all data up to the
    current row in training, which means detection period data affects the training baseline.

    Test Scenario:
    - 30 days of normal data: 100 rows per day (baseline pattern)
    - 7 days of anomalous data: 500 rows per day (5x spike) in the detection period
    - Training period: 30 days
    - Detection period: 7 days
    - Time bucket: Daily aggregation
    - Sensitivity: 3 (default)

    Current Behavior:
    - The test FAILS (anomaly detected) because the 5x spike is large enough to be detected
      even when included in the cumulative training average.

    Expected Behavior with exclude_detection_period_from_training flag:
    - With the flag enabled, the detection period would be excluded from training,
      making the anomaly detection more sensitive and reliable.
    - This would be especially important for gradual anomalies that might be masked
      by the cumulative training approach.
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
        for _ in range(500):
            anomalous_data.append({TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)})

    data = normal_data + anomalous_data

    test_args = {
        **DBT_TEST_ARGS,
        "time_bucket": {"period": "day", "count": 1},
        "training_period": {"period": "day", "count": 30},
        "detection_period": {"period": "day", "count": 7},
        "sensitivity": 3,
    }

    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args,
        data=data,
    )

    # Current behavior: Test FAILS (anomaly detected) because the spike is large enough
    # Even though the detection period is included in training, the 5x spike is still detected
    assert test_result["status"] == "fail", (
        "Test should FAIL in current implementation. "
        "The 5x spike is large enough to be detected even with detection period in training."
    )

    # TODO: When the exclude_detection_period_from_training flag is implemented,
    # add a second test here that sets the flag to True:
    # test_args_with_exclusion = {
    #     **test_args,
    #     "exclude_detection_period_from_training": True,
    # }
    # test_result_with_exclusion = dbt_project.test( (important-comment)
    #     test_id,
    #     DBT_TEST_NAME,
    #     test_args_with_exclusion,
    #     test_vars={"force_metrics_backfill": True},
    # )
    # With the flag, the anomaly should still be detected (test fails)
    # but the detection would be more reliable and sensitive.
