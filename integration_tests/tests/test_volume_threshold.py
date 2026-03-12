from datetime import datetime, timedelta

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.volume_threshold"
DBT_TEST_ARGS = {
    "timestamp_column": TIMESTAMP_COLUMN,
    "time_bucket": {"period": "day", "count": 1},
    "days_back": 14,
    "backfill_days": 14,
}


def _generate_stable_data(rows_per_day=100, days_back=14):
    """Generate data with a consistent number of rows per day bucket."""
    utc_today = datetime.utcnow().date()
    data = []
    for cur_date in generate_dates(base_date=utc_today, days_back=days_back):
        for _ in range(rows_per_day):
            data.append({TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)})
    return data


def test_stable_volume_passes(test_id: str, dbt_project: DbtProject):
    """Consistent row counts across buckets should pass."""
    data = _generate_stable_data(rows_per_day=100)
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "pass"


def test_large_spike_fails(test_id: str, dbt_project: DbtProject):
    """A large spike in row count (>10% default error threshold) should fail."""
    utc_today = datetime.utcnow().date()
    yesterday = utc_today - timedelta(days=1)
    data = []
    # Previous days: 100 rows each
    for cur_date in generate_dates(base_date=utc_today, days_back=14):
        if cur_date < yesterday:
            for _ in range(100):
                data.append({TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)})
    # Yesterday (current bucket): 100 rows
    for _ in range(100):
        data.append({TIMESTAMP_COLUMN: yesterday.strftime(DATE_FORMAT)})
    # Today (current bucket): 150 rows (50% spike)
    for _ in range(150):
        data.append({TIMESTAMP_COLUMN: utc_today.strftime(DATE_FORMAT)})

    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] != "pass"


def test_large_drop_fails(test_id: str, dbt_project: DbtProject):
    """A large drop in row count (>10% default error threshold) should fail."""
    utc_today = datetime.utcnow().date()
    yesterday = utc_today - timedelta(days=1)
    data = []
    # Previous days: 100 rows each
    for cur_date in generate_dates(base_date=utc_today, days_back=14):
        if cur_date < yesterday:
            for _ in range(100):
                data.append({TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)})
    # Yesterday (previous bucket): 100 rows
    for _ in range(100):
        data.append({TIMESTAMP_COLUMN: yesterday.strftime(DATE_FORMAT)})
    # Today (current bucket): 50 rows (50% drop)
    for _ in range(50):
        data.append({TIMESTAMP_COLUMN: utc_today.strftime(DATE_FORMAT)})

    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] != "pass"


def test_direction_spike_ignores_drop(test_id: str, dbt_project: DbtProject):
    """With direction=spike, a drop should not trigger a failure."""
    utc_today = datetime.utcnow().date()
    yesterday = utc_today - timedelta(days=1)
    data = []
    # Previous days: 100 rows each
    for cur_date in generate_dates(base_date=utc_today, days_back=14):
        if cur_date < yesterday:
            for _ in range(100):
                data.append({TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)})
    # Yesterday: 100 rows
    for _ in range(100):
        data.append({TIMESTAMP_COLUMN: yesterday.strftime(DATE_FORMAT)})
    # Today: 50 rows (50% drop)
    for _ in range(50):
        data.append({TIMESTAMP_COLUMN: utc_today.strftime(DATE_FORMAT)})

    test_args = {**DBT_TEST_ARGS, "direction": "spike"}
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"


def test_direction_drop_ignores_spike(test_id: str, dbt_project: DbtProject):
    """With direction=drop, a spike should not trigger a failure."""
    utc_today = datetime.utcnow().date()
    yesterday = utc_today - timedelta(days=1)
    data = []
    # Previous days: 100 rows each
    for cur_date in generate_dates(base_date=utc_today, days_back=14):
        if cur_date < yesterday:
            for _ in range(100):
                data.append({TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)})
    # Yesterday: 100 rows
    for _ in range(100):
        data.append({TIMESTAMP_COLUMN: yesterday.strftime(DATE_FORMAT)})
    # Today: 150 rows (50% spike)
    for _ in range(150):
        data.append({TIMESTAMP_COLUMN: utc_today.strftime(DATE_FORMAT)})

    test_args = {**DBT_TEST_ARGS, "direction": "drop"}
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"


def test_min_row_count_skips_small_baseline(test_id: str, dbt_project: DbtProject):
    """When previous bucket has fewer rows than min_row_count, check is skipped (pass)."""
    utc_today = datetime.utcnow().date()
    yesterday = utc_today - timedelta(days=1)
    data = []
    # Previous days: only 5 rows each (below default min_row_count=100)
    for cur_date in generate_dates(base_date=utc_today, days_back=14):
        if cur_date < yesterday:
            for _ in range(5):
                data.append({TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)})
    # Yesterday: 5 rows
    for _ in range(5):
        data.append({TIMESTAMP_COLUMN: yesterday.strftime(DATE_FORMAT)})
    # Today: 50 rows (huge spike but baseline is too small)
    for _ in range(50):
        data.append({TIMESTAMP_COLUMN: utc_today.strftime(DATE_FORMAT)})

    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "pass"


def test_custom_thresholds(test_id: str, dbt_project: DbtProject):
    """Custom thresholds should control the sensitivity of the test."""
    utc_today = datetime.utcnow().date()
    yesterday = utc_today - timedelta(days=1)
    data = []
    # Previous days: 100 rows each
    for cur_date in generate_dates(base_date=utc_today, days_back=14):
        if cur_date < yesterday:
            for _ in range(100):
                data.append({TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)})
    # Yesterday: 100 rows
    for _ in range(100):
        data.append({TIMESTAMP_COLUMN: yesterday.strftime(DATE_FORMAT)})
    # Today: 108 rows (8% change)
    for _ in range(108):
        data.append({TIMESTAMP_COLUMN: utc_today.strftime(DATE_FORMAT)})

    # With default thresholds (warn=5, error=10), 8% should warn but not error
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "warn"

    # With high thresholds (warn=20, error=50), 8% should pass
    test_args_high = {
        **DBT_TEST_ARGS,
        "warn_threshold_percent": 20,
        "error_threshold_percent": 50,
    }
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args_high,
        test_vars={"force_metrics_backfill": True},
    )
    assert test_result["status"] == "pass"


def test_where_expression(test_id: str, dbt_project: DbtProject):
    """The where_expression should filter which rows are counted."""
    utc_today = datetime.utcnow().date()
    yesterday = utc_today - timedelta(days=1)
    data = []
    # Previous days: 100 rows of category A each
    for cur_date in generate_dates(base_date=utc_today, days_back=14):
        if cur_date < yesterday:
            for _ in range(100):
                data.append(
                    {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT), "category": "a"}
                )
    # Yesterday: 100 rows of category A
    for _ in range(100):
        data.append(
            {TIMESTAMP_COLUMN: yesterday.strftime(DATE_FORMAT), "category": "a"}
        )
    # Today: 100 rows of category A (stable) + 200 rows of category B (noise)
    for _ in range(100):
        data.append(
            {TIMESTAMP_COLUMN: utc_today.strftime(DATE_FORMAT), "category": "a"}
        )
    for _ in range(200):
        data.append(
            {TIMESTAMP_COLUMN: utc_today.strftime(DATE_FORMAT), "category": "b"}
        )

    # Without filter: total today = 300 vs 100 yesterday -> big spike -> fail
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] != "pass"

    # With filter on category A: 100 today vs 100 yesterday -> stable -> pass
    test_args_filtered = {
        **DBT_TEST_ARGS,
        "where_expression": "category = 'a'",
    }
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args_filtered,
        test_vars={"force_metrics_backfill": True},
    )
    assert test_result["status"] == "pass"
