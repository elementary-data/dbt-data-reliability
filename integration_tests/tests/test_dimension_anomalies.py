import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pytest
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.dimension_anomalies"
DBT_TEST_ARGS = {"timestamp_column": TIMESTAMP_COLUMN, "dimensions": ["superhero"]}

# This returns data points used in the latest anomaly test.
# T-SQL does not support LIMIT; use TOP instead when target is fabric/sqlserver.
ANOMALY_TEST_POINTS_QUERY = """
    with latest_elementary_test_result as (
        select {top_clause}id
        from {{{{ ref("elementary_test_results") }}}}
        where lower(table_name) = lower('{test_id}')
        order by created_at desc
        {limit_clause}
    )

    select result_row
    from {{{{ ref("test_result_rows") }}}}
    where elementary_test_results_id in (select * from latest_elementary_test_result)
"""


def get_latest_anomaly_test_points(dbt_project: DbtProject, test_id: str):
    sl = dbt_project.select_limit(1)
    query = ANOMALY_TEST_POINTS_QUERY.format(
        test_id=test_id,
        top_clause=sl.top,
        limit_clause=sl.limit,
    )
    results = dbt_project.run_query(query)
    return [json.loads(result["result_row"]) for result in results]


def test_anomalyless_dimension_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Spiderman"]
    ]
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "pass"

    # Dimension anomalies only stores anomalous rows (unlike other anomaly tests) - so we should get 0 rows for a passing test.
    anomaly_test_points = get_latest_anomaly_test_points(dbt_project, test_id)
    assert len(anomaly_test_points) == 0


def test_dimension_anomalies_with_timestamp_as_sql_expression(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Spiderman"]
    ]
    test_args = {
        "timestamp_column": "case when updated_at is not null then updated_at else updated_at end",
        "dimensions": ["superhero"],
    }
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"


def test_anomalous_dimension_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for superhero in ["Superman", "Superman", "Superman", "Spiderman"]
    ]

    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in training_dates
        for superhero in ["Superman", "Spiderman"]
    ]

    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "fail"

    anomaly_test_points = get_latest_anomaly_test_points(dbt_project, test_id)

    # Only dimension values with anomalies are stored in the test points
    dimension_values = set([x["dimension_value"] for x in anomaly_test_points])

    superman_anomaly_test_points = [
        x for x in anomaly_test_points if x["dimension_value"] == "Superman"
    ]

    assert len(dimension_values) == 1
    assert "Superman" in dimension_values
    assert len(anomaly_test_points) == len(superman_anomaly_test_points)
    assert any(x["is_anomalous"] for x in superman_anomaly_test_points)


def test_dimensions_anomalies_with_where_parameter(
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
            ("DC", "Superman"),
            ("DC", "Superman"),
            ("DC", "Superman"),
            ("Marvel", "Spiderman"),
        ]
    ] + [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "universe": universe,
            "superhero": superhero,
        }
        for cur_date in training_dates
        for universe, superhero in [("DC", "Superman"), ("Marvel", "Spiderman")]
    ]

    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "fail"

    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        test_vars={"force_metrics_backfill": True},
        test_config={"where": "universe = 'Marvel'"},
    )
    assert test_result["status"] == "pass"

    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        test_vars={"force_metrics_backfill": True},
        test_config={"where": "universe = 'DC'"},
    )
    assert test_result["status"] == "fail"


def test_dimension_anomalies_with_timestamp_exclude_final_results(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(3))
        for superhero in ["Superman", "Spiderman"]
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1), days_back=2)
        for superhero in ["Spiderman"]
    ] * 30
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1), days_back=2)
        for superhero in ["Superman"]
    ] * 15

    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "fail"
    assert test_result["failures"] == 2

    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "dimensions": ["superhero"],
        "exclude_final_results": '{{ elementary.escape_reserved_keywords("value") }} > 15',
    }
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "fail"
    assert test_result["failures"] == 1

    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "dimensions": ["superhero"],
        "exclude_final_results": '{{ elementary.escape_reserved_keywords("average") }} > 3',
    }
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "fail"
    assert test_result["failures"] == 1


# Test for exclude_detection_period_from_training functionality
# This test demonstrates the use case where:
# 1. Detection period contains anomalous distribution data that would normally be included in training
# 2. With exclude_detection=False: anomaly is missed (test passes) because training includes the anomaly
# 3. With exclude_detection=True: anomaly is detected (test fails) because training excludes the anomaly
@pytest.mark.parametrize(
    "exclude_detection,expected_status",
    [
        (False, "pass"),  # include detection in training → anomaly absorbed
        (True, "fail"),  # exclude detection from training → anomaly detected
    ],
    ids=[
        "exclude_false",
        "exclude_true",
    ],  # Shortened to stay under Postgres 63-char limit
)
def test_anomaly_in_detection_period(
    test_id: str,
    dbt_project: DbtProject,
    exclude_detection: bool,
    expected_status: str,
):
    """
    Test the exclude_detection_period_from_training flag functionality for dimension anomalies.

    Scenario:
    - 30 days of normal data with variance (45/50/55 Superman, 55/50/45 Spiderman pattern)
    - 7 days of anomalous data (72 Superman, 28 Spiderman per day) in detection period
    - Without exclusion: anomaly gets included in training baseline, test passes (misses anomaly)
    - With exclusion: anomaly excluded from training, test fails (detects anomaly)

    Note: Parametrize IDs are shortened to avoid Postgres 63-character identifier limit.
    """
    utc_now = datetime.utcnow().date()

    # Generate 30 days of normal data with variance (45/50/55 pattern for Superman)
    normal_pattern = [45, 50, 55]
    normal_data = []
    for i in range(30):
        date = utc_now - timedelta(days=37 - i)
        superman_count = normal_pattern[i % 3]
        spiderman_count = 100 - superman_count
        normal_data.extend(
            [
                {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT), "superhero": "Superman"}
                for _ in range(superman_count)
            ]
        )
        normal_data.extend(
            [
                {
                    TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
                    "superhero": "Spiderman",
                }
                for _ in range(spiderman_count)
            ]
        )

    # Generate 7 days of anomalous data (72 Superman, 28 Spiderman per day) - this will be in detection period
    anomalous_data = []
    for i in range(7):
        date = utc_now - timedelta(days=7 - i)
        anomalous_data.extend(
            [
                {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT), "superhero": "Superman"}
                for _ in range(72)
            ]
        )
        anomalous_data.extend(
            [
                {
                    TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT),
                    "superhero": "Spiderman",
                }
                for _ in range(28)
            ]
        )

    all_data = normal_data + anomalous_data

    test_args = {
        **DBT_TEST_ARGS,
        "training_period": {"period": "day", "count": 30},
        "detection_period": {"period": "day", "count": 7},
        "time_bucket": {"period": "day", "count": 1},
        "sensitivity": 5,
    }
    if exclude_detection:
        test_args["exclude_detection_period_from_training"] = True

    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args,
        data=all_data,
    )

    assert test_result["status"] == expected_status


def _generate_current_bucket_data(utc_today: date, today_superheroes: List[str]):
    """One Superman and one Spiderman per day until yesterday, and today_superheroes today.

    Today is the bucket that is still in progress when the test runs.
    """
    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT), "superhero": superhero}
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Spiderman"]
    ]
    data += [
        {TIMESTAMP_COLUMN: utc_today.strftime(DATE_FORMAT), "superhero": superhero}
        for superhero in today_superheroes
    ]
    return data


def test_include_current_bucket_detects_anomaly_in_current_bucket(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    data = _generate_current_bucket_data(
        utc_today, ["Superman", "Superman", "Superman", "Spiderman"]
    )

    # By default only complete buckets are tested, so today's anomaly is not detected yet.
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "pass"

    test_args = {**DBT_TEST_ARGS, "include_current_bucket": True}
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "fail"

    anomaly_test_points = get_latest_anomaly_test_points(dbt_project, test_id)
    anomalous_points = [x for x in anomaly_test_points if x["is_anomalous"]]
    assert set(x["dimension_value"] for x in anomalous_points) == {"Superman"}
    assert all(
        x["bucket_start"].startswith(utc_today.strftime("%Y-%m-%d"))
        for x in anomalous_points
    )


def test_include_current_bucket_passes_on_normal_current_bucket(
    test_id: str, dbt_project: DbtProject
):
    data = _generate_current_bucket_data(
        datetime.utcnow().date(), ["Superman", "Spiderman"]
    )
    test_args = {**DBT_TEST_ARGS, "include_current_bucket": True}
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"


def test_include_current_bucket_ignores_dimension_not_arrived_yet(
    test_id: str, dbt_project: DbtProject
):
    """A dimension without rows in the current bucket has not arrived yet, so it is not a drop to zero."""
    data = _generate_current_bucket_data(datetime.utcnow().date(), ["Spiderman"])
    test_args = {**DBT_TEST_ARGS, "include_current_bucket": True}
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"


# Redshift does not support monthly time buckets.
@pytest.mark.skip_targets(["redshift"])
def test_include_current_bucket_monthly_snapshot_upload(
    test_id: str, dbt_project: DbtProject
):
    """Monthly uploads per country: a short upload is detected in the month it lands."""
    current_month, *previous_months = _get_previous_bucket_starts("month", 12)

    rows_per_upload = {"PL": 100, "MY": 20}
    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: month_start.strftime(DATE_FORMAT), "country": country}
        for month_start in previous_months
        for country, rows in rows_per_upload.items()
        for _ in range(rows)
    ]
    # This month PL delivered a normal upload and MY a short one.
    data += [
        {TIMESTAMP_COLUMN: current_month.strftime(DATE_FORMAT), "country": country}
        for country, rows in {"PL": 100, "MY": 10}.items()
        for _ in range(rows)
    ]

    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "dimensions": ["country"],
        "time_bucket": {"period": "month", "count": 1},
        "training_period": {"period": "day", "count": 400},
        "detection_period": {"period": "day", "count": 31},
    }

    # By default this month is only tested once it has ended.
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"

    test_args["include_current_bucket"] = True
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "fail"

    anomaly_test_points = get_latest_anomaly_test_points(dbt_project, test_id)
    anomalous_points = [x for x in anomaly_test_points if x["is_anomalous"]]
    assert set(x["dimension_value"] for x in anomalous_points) == {"MY"}


def _get_previous_bucket_starts(
    period: str, count: int, base_date: Optional[date] = None
) -> List[date]:
    """The start of the week or month of base_date (default today), followed by the starts of the `count` before it."""
    base_date = base_date or datetime.utcnow().date()
    if period == "week":
        current_week_start = base_date - timedelta(days=base_date.weekday())
        return [current_week_start - timedelta(weeks=i) for i in range(count + 1)]
    bucket_starts = [base_date.replace(day=1)]
    for _ in range(count):
        previous = bucket_starts[-1] - timedelta(days=1)
        bucket_starts.append(previous.replace(day=1))
    return bucket_starts


@pytest.mark.parametrize("period", ["week", "month"])
def test_buckets_stay_aligned_on_rerun(
    test_id: str, dbt_project: DbtProject, target: str, period: str
):
    """On a rerun the backfill window starts mid-bucket; buckets must still be calendar weeks/months."""
    if target == "redshift" and period == "month":
        pytest.skip("Redshift does not support monthly time buckets.")
    # Uploads on the first day of each bucket, for the buckets before the current one.
    bucket_starts = _get_previous_bucket_starts(period, 12)[1:]

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: bucket_start.strftime(DATE_FORMAT), "country": country}
        for bucket_start in bucket_starts
        for country, rows in {"PL": 100, "MY": 20}.items()
        for _ in range(rows)
    ]
    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "dimensions": ["country"],
        "time_bucket": {"period": period, "count": 1},
        "training_period": {"period": "day", "count": 400},
        "detection_period": {"period": "day", "count": 31},
    }

    first_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert first_result["status"] == "pass"
    second_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert second_result["status"] == "pass"


@pytest.mark.parametrize(
    "period,first_run_at,run_at",
    [
        ("week", "2026-10-07T12:00:00", "2026-10-14T12:00:00"),
        # October has more days than the 30 backfill_days of monthly buckets.
        ("month", "2026-09-10T12:00:00", "2026-10-15T12:00:00"),
    ],
    ids=["week", "month"],
)
def test_include_current_bucket_tests_previous_bucket(
    test_id: str,
    dbt_project: DbtProject,
    target: str,
    period: str,
    first_run_at: str,
    run_at: str,
):
    """MY skipped the previous bucket. Testing the current bucket must not stop testing the previous one."""
    if target == "redshift" and period == "month":
        pytest.skip("Redshift does not support monthly time buckets.")
    test_id = test_id.replace("[", "_").replace("]", "_")
    current_bucket, previous_bucket, *older_buckets = _get_previous_bucket_starts(
        period, 12, base_date=datetime.fromisoformat(run_at).date()
    )
    rows_per_bucket = {bucket: {"PL": 100, "MY": 20} for bucket in older_buckets}
    rows_per_bucket[previous_bucket] = {"PL": 100}
    rows_per_bucket[current_bucket] = {"PL": 100, "MY": 20}
    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: bucket_start.strftime(DATE_FORMAT), "country": country}
        for bucket_start, rows_per_country in rows_per_bucket.items()
        for country, rows in rows_per_country.items()
        for _ in range(rows)
    ]
    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "dimensions": ["country"],
        "time_bucket": {"period": period, "count": 1},
        "training_period": {"period": "day", "count": 400},
        "include_current_bucket": True,
    }

    # A run during the previous bucket stores MY in the metrics history, so the next run
    # fills in a zero for MY once the previous bucket is complete.
    first_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args,
        data=data,
        test_vars={"custom_run_started_at": first_run_at},
    )
    assert first_result["status"] == "pass"
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args,
        test_vars={"custom_run_started_at": run_at},
    )
    assert test_result["status"] == "fail"

    # Only checks that the bucket is complete, since some adapters start weeks on Sunday.
    anomaly_test_points = get_latest_anomaly_test_points(dbt_project, test_id)
    anomalous_points = [x for x in anomaly_test_points if x["is_anomalous"]]
    assert set(x["dimension_value"] for x in anomalous_points) == {"MY"}
    assert all(x["bucket_end"][:10] <= run_at[:10] for x in anomalous_points)
