from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.all_columns_anomalies"
DBT_TEST_ARGS = {
    "timestamp_column": TIMESTAMP_COLUMN,
    "column_anomalies": ["null_count"],
}


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalyless_all_columns_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Batman"]
    ]
    test_results = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, multiple_results=True
    )
    assert all([res["status"] == "pass" for res in test_results])


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalous_all_columns_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), "superhero": None}
        for _ in range(3)
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in training_dates
        for superhero in ["Superman", "Batman"]
    ]

    test_results = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, multiple_results=True
    )
    col_to_status = {res["column_name"].lower(): res["status"] for res in test_results}
    assert col_to_status == {"superhero": "fail", TIMESTAMP_COLUMN: "pass"}


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_all_columns_anomalies_with_where_parameter(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "universe": universe,
            "superhero": superhero,
        }
        for universe, superhero in [
            ("DC", None),
            ("DC", None),
            ("DC", None),
            ("Marvel", "Spiderman"),
        ]
    ] + [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "universe": universe,
            "superhero": superhero,
        }
        for cur_date in training_dates
        for universe, superhero in [
            ("DC", "Superman"),
            ("DC", "Batman"),
            ("DC", None),
            ("Marvel", "Spiderman"),
        ]
    ]

    test_results = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, multiple_results=True
    )
    col_to_status = {res["column_name"].lower(): res["status"] for res in test_results}
    assert col_to_status == {
        "superhero": "fail",
        TIMESTAMP_COLUMN: "pass",
        "universe": "pass",
    }

    test_results = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        multiple_results=True,
        test_vars={"force_metrics_backfill": True},
        test_config={"where": "universe = 'Marvel'"},
    )
    assert all([res["status"] == "pass" for res in test_results])

    test_results = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        multiple_results=True,
        test_vars={"force_metrics_backfill": True},
        test_config={"where": "universe = 'DC'"},
    )
    col_to_status = {res["column_name"].lower(): res["status"] for res in test_results}
    assert col_to_status == {
        "superhero": "fail",
        TIMESTAMP_COLUMN: "pass",
        "universe": "pass",
    }


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalyless_all_columns_anomalies_all_monitors_sanity(
    test_id: str, dbt_project: DbtProject
):
    # Tests above run on a specific monitor (null count) since it's easier to test accurately.
    # Nonetheless, it's useful to actually do a sanity on the full monitors list since it can detect things such as syntax errors.
    # Down the line it may be good to actually have a test per column monitor.

    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,  # String column
            "flights": 5,  # Numeric column
            "has_flown": True,  # Boolean column
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Batman"]
    ]
    test_args = {"timestamp_column": TIMESTAMP_COLUMN}
    test_results = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, multiple_results=True
    )
    assert all([res["status"] == "pass" for res in test_results])


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
@pytest.mark.parametrize(
    "exclude_detection,expected_status",
    [
        (False, "pass"),
        (True, "fail"),
    ],
    ids=["without_exclusion", "with_exclusion"],
)
def test_anomaly_in_detection_period(
    test_id: str,
    dbt_project: DbtProject,
    exclude_detection: bool,
    expected_status: str,
):
    """
    Test the exclude_detection_period_from_training flag functionality for column anomalies.

    Scenario:
    - 30 days of normal data with variance in null_count pattern (8, 10, 12 nulls per day)
    - 7 days of anomalous data (20 nulls per day) in detection period
    - Without exclusion (exclude_detection=False): anomaly gets included in training baseline, test passes
    - With exclusion (exclude_detection=True): anomaly excluded from training, test fails (detects anomaly)
    """
    utc_now = datetime.now(timezone.utc)

    # Generate 30 days of normal data with variance in null_count (8, 10, 12 pattern)
    normal_pattern = [8, 10, 12]
    normal_data = []
    for i in range(30):
        date = utc_now - timedelta(days=37 - i)
        null_count = normal_pattern[i % 3]
        normal_data.extend(
            [
                {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT), "superhero": None}
                for _ in range(null_count)
            ]
        )
        normal_data.extend(
            [
                {
                    TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
                    "superhero": "Superman" if i % 2 == 0 else "Batman",
                }
                for _ in range(40 - null_count)
            ]
        )

    # Generate 7 days of anomalous data (20 nulls per day) - 100% increase from mean
    anomalous_data = []
    for i in range(7):
        date = utc_now - timedelta(days=7 - i)
        anomalous_data.extend(
            [
                {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT), "superhero": None}
                for _ in range(20)
            ]
        )
        anomalous_data.extend(
            [
                {
                    TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
                    "superhero": "Superman" if i % 2 == 0 else "Batman",
                }
                for _ in range(20)
            ]
        )

    all_data = normal_data + anomalous_data

    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "column_anomalies": ["null_count"],
        "training_period": {"period": "day", "count": 30},
        "detection_period": {"period": "day", "count": 7},
        "time_bucket": {"period": "day", "count": 1},
        "sensitivity": 5,
    }

    if exclude_detection:
        test_args["exclude_detection_period_from_training"] = True

    test_results = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args,
        data=all_data,
        multiple_results=True,
    )

    superhero_result = next(
        (res for res in test_results if res["column_name"].lower() == "superhero"),
        None,
    )
    assert superhero_result is not None, "superhero column result not found"
    assert (
        superhero_result["status"] == expected_status
    ), f"Expected status '{expected_status}' but got '{superhero_result['status']}' (exclude_detection={exclude_detection})"
