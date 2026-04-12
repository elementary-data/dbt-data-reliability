from datetime import datetime, timedelta
from typing import Any, Dict, List

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.column_value_anomalies"
DBT_TEST_ARGS = {
    "timestamp_column": TIMESTAMP_COLUMN,
}


def test_anomalyless_column_value_anomalies(test_id: str, dbt_project: DbtProject):
    """Test that normal, consistent numeric data produces no anomalies (test passes)."""
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "amount": 100,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for _ in range(5)
    ]
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, test_column="amount"
    )
    assert test_result["status"] == "pass"


def test_anomalous_column_value_anomalies(test_id: str, dbt_project: DbtProject):
    """Test that an extreme outlier in the detection period is flagged (test fails).

    Training data: values around 100 (95-105) for 30 days.
    Detection data: includes a value of 10000, which is a clear outlier.
    """
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    # Training data: consistent values around 100
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "amount": amount,
        }
        for cur_date in training_dates
        for amount in [95, 100, 105, 100, 100]
    ]
    # Detection data: includes an extreme outlier
    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "amount": amount,
        }
        for amount in [100, 100, 10000]
    ]

    test_args = {
        **DBT_TEST_ARGS,
        "anomaly_sensitivity": 3,
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="amount"
    )
    assert test_result["status"] == "fail"


def test_column_value_anomalies_spike_direction(test_id: str, dbt_project: DbtProject):
    """Test anomaly_direction='spike' only flags values above threshold.

    Training data: values around 100.
    Detection data: one very high value (10000) and one very low value (-10000).
    With spike direction, only the high value should be flagged.
    With drop direction, only the low value should be flagged.
    """
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    # Training data: consistent values around 100
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "amount": amount,
        }
        for cur_date in training_dates
        for amount in [95, 100, 105, 100, 100]
    ]
    # Detection data: one extreme high, one extreme low
    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "amount": 10000,
        },
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "amount": -10000,
        },
    ]

    # spike direction: should fail (10000 is a spike)
    test_args_spike = {
        **DBT_TEST_ARGS,
        "anomaly_sensitivity": 3,
        "anomaly_direction": "spike",
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args_spike, data=data, test_column="amount"
    )
    assert test_result["status"] == "fail"

    # drop direction: should fail (-10000 is a drop)
    test_args_drop = {
        **DBT_TEST_ARGS,
        "anomaly_sensitivity": 3,
        "anomaly_direction": "drop",
    }
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args_drop,
        test_column="amount",
        test_vars={"force_metrics_backfill": True},
    )
    assert test_result["status"] == "fail"


def test_column_value_anomalies_with_where_expression(
    test_id: str, dbt_project: DbtProject
):
    """Test that where_expression filters data correctly.

    Two categories: 'normal' has consistent values, 'outlier' has extreme values.
    Filtering to 'normal' category should pass; filtering to 'outlier' category
    should fail due to the extreme detection-period values.
    """
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    # Training data for both categories
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "category": category,
            "amount": 100,
        }
        for cur_date in training_dates
        for category in ["normal", "outlier"]
        for _ in range(3)
    ]
    # Detection data: normal category is fine, outlier category has extreme value
    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "category": "normal",
            "amount": 100,
        },
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "category": "outlier",
            "amount": 10000,
        },
    ]

    # Without where: should fail (outlier category has extreme value)
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, test_column="amount"
    )
    assert test_result["status"] == "fail"

    # With where filtering to normal: should pass
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        test_column="amount",
        test_vars={"force_metrics_backfill": True},
        test_config={"where": "category = 'normal'"},
    )
    assert test_result["status"] == "pass"


def test_column_value_anomalies_sensitivity(test_id: str, dbt_project: DbtProject):
    """Test that anomaly_sensitivity threshold controls detection.

    Training data: values around 100 (sample stddev ~3.5).
    Detection data: value of 130 (z-score ~8.6).
    With sensitivity=3, should fail. With sensitivity=10, should pass.
    """
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    # Training data: values with known variance
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "amount": amount,
        }
        for cur_date in training_dates
        for amount in [95, 100, 105, 100, 100]
    ]
    # Detection data: moderately high value
    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "amount": 130,
        },
    ]

    # Low sensitivity: should fail (130 is ~8.6 stddevs from mean ~100, stddev ~3.5)
    test_args_low = {
        **DBT_TEST_ARGS,
        "anomaly_sensitivity": 3,
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args_low, data=data, test_column="amount"
    )
    assert test_result["status"] == "fail"

    # High sensitivity: should pass (130 is within 10 stddevs)
    test_args_high = {
        **DBT_TEST_ARGS,
        "anomaly_sensitivity": 10,
    }
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args_high,
        test_column="amount",
        test_vars={"force_metrics_backfill": True},
    )
    assert test_result["status"] == "pass"


def test_column_value_anomalies_spike_ignores_drop(
    test_id: str, dbt_project: DbtProject
):
    """Test that anomaly_direction='spike' ignores drop outliers.

    Training data: values around 100.
    Detection data: only a very low value (-10000), no spike.
    With spike direction, this drop should be ignored → test passes.
    """
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "amount": amount,
        }
        for cur_date in training_dates
        for amount in [95, 100, 105, 100, 100]
    ]
    # Detection data: only a drop outlier (no spike)
    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "amount": -10000,
        },
    ]

    test_args = {
        **DBT_TEST_ARGS,
        "anomaly_sensitivity": 3,
        "anomaly_direction": "spike",
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="amount"
    )
    assert test_result["status"] == "pass"


def test_column_value_anomalies_drop_ignores_spike(
    test_id: str, dbt_project: DbtProject
):
    """Test that anomaly_direction='drop' ignores spike outliers.

    Training data: values around 100.
    Detection data: only a very high value (10000), no drop.
    With drop direction, this spike should be ignored → test passes.
    """
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "amount": amount,
        }
        for cur_date in training_dates
        for amount in [95, 100, 105, 100, 100]
    ]
    # Detection data: only a spike outlier (no drop)
    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "amount": 10000,
        },
    ]

    test_args = {
        **DBT_TEST_ARGS,
        "anomaly_sensitivity": 3,
        "anomaly_direction": "drop",
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="amount"
    )
    assert test_result["status"] == "pass"


def test_column_value_anomalies_with_seasonality(test_id: str, dbt_project: DbtProject):
    """Test that seasonality=day_of_week uses per-day-of-week baselines.

    Scenario: Weekdays have values ~10, weekends have values ~1000.
    Detection period: a weekend day with value 1000.
    Without seasonality, the blended baseline (mean ~300, stddev ~400) would
    flag 1000 as anomalous. With day_of_week seasonality, weekend baseline
    is ~1000 so the value is normal.
    """
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(
        base_date=utc_today - timedelta(1), days_back=60
    )

    data: List[Dict[str, Any]] = []
    # Training data: weekdays ~10, weekends ~1000 (wide gap)
    for cur_date in training_dates:
        day_of_week = cur_date.weekday()  # 0=Monday, 6=Sunday
        if day_of_week >= 5:  # Weekend
            values = [990, 1000, 1010]
        else:  # Weekday
            values = [8, 10, 12]
        for amount in values:
            data.append(
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "amount": amount,
                }
            )

    # Detection data: always use a weekend-like value to ensure the test
    # is meaningful regardless of what day test_date falls on.
    # We pick value 1000 which matches weekend pattern but is far from
    # the blended mean.
    test_day_of_week = test_date.weekday()
    if test_day_of_week >= 5:
        detection_value = 1000
    else:
        detection_value = 10
    data.append(
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "amount": detection_value,
        }
    )

    # With seasonality: should pass (value matches day-of-week pattern)
    test_args_seasonal = {
        **DBT_TEST_ARGS,
        "anomaly_sensitivity": 3,
        "seasonality": "day_of_week",
        "training_period": {"period": "day", "count": 60},
    }
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args_seasonal,
        data=data,
        test_column="amount",
    )
    assert test_result["status"] == "pass"


def test_column_value_anomalies_with_training_period(
    test_id: str, dbt_project: DbtProject
):
    """Test that training_period controls the baseline window.

    30 days of low values (~10), then 7 days of high values (~1000),
    then detection with value 1000.
    With training_period=7 days (only recent high values): should pass.
    With training_period=37 days (includes old low values): should fail
    because 1000 is far from the blended mean (~163, stddev ~327, z≈2.6
    but the 30-day low-value majority pulls the mean down enough to exceed
    the threshold with sensitivity=2).
    """
    utc_today = datetime.utcnow().date()
    test_date = utc_today - timedelta(1)

    data: List[Dict[str, Any]] = []

    # 30 days of low values (old data)
    for i in range(30):
        cur_date = utc_today - timedelta(days=37 - i)
        for amount in [8, 10, 12]:
            data.append(
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "amount": amount,
                }
            )

    # 7 days of high values (recent training data)
    for i in range(7):
        cur_date = utc_today - timedelta(days=7 - i)
        if cur_date >= test_date:
            continue
        for amount in [990, 1000, 1010]:
            data.append(
                {
                    TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
                    "amount": amount,
                }
            )

    # Detection: value consistent with recent training
    data.append(
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "amount": 1000,
        }
    )

    # Short training period (7 days) - baseline is ~1000, detection value is normal
    test_args = {
        **DBT_TEST_ARGS,
        "anomaly_sensitivity": 3,
        "training_period": {"period": "day", "count": 7},
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="amount"
    )
    assert test_result["status"] == "pass"

    # Control: long training period (37 days) includes the 30 days of low values.
    # Blended baseline is dominated by ~10 values, making 1000 a clear outlier.
    # 90 values of ~10 + 18 values of ~1000 → mean ≈ 175, stddev ≈ 330.
    # Z-score for 1000 ≈ (1000-175)/330 ≈ 2.5. With sensitivity=2, this fails.
    control_args = {
        **DBT_TEST_ARGS,
        "anomaly_sensitivity": 2,
        "training_period": {"period": "day", "count": 37},
    }
    control_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        control_args,
        test_column="amount",
        test_vars={"force_metrics_backfill": True},
    )
    assert control_result["status"] == "fail"
