from datetime import datetime, timedelta

from data_generator import DATE_FORMAT
from dbt_project import DbtProject

TEST_NAME = "elementary.data_freshness_sla"
TIMESTAMP_COLUMN = "updated_at"


def test_fresh_data_passes(test_id: str, dbt_project: DbtProject):
    """Data updated today should pass when the SLA deadline has already passed."""
    utc_now = datetime.utcnow()
    data = [
        {TIMESTAMP_COLUMN: utc_now.strftime(DATE_FORMAT)},
        {TIMESTAMP_COLUMN: (utc_now - timedelta(hours=1)).strftime(DATE_FORMAT)},
    ]
    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "sla_time": "11:59pm",
        "timezone": "UTC",
    }
    test_result = dbt_project.test(test_id, TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"


def test_stale_data_fails(test_id: str, dbt_project: DbtProject):
    """Data only from previous days should fail when today's SLA deadline has passed."""
    utc_now = datetime.utcnow()
    yesterday = utc_now - timedelta(days=2)
    data = [
        {TIMESTAMP_COLUMN: yesterday.strftime(DATE_FORMAT)},
        {TIMESTAMP_COLUMN: (yesterday - timedelta(hours=5)).strftime(DATE_FORMAT)},
    ]
    # Use 12:01am UTC (= 00:01 UTC) so the deadline is always in the past when
    # CI runs (typically 07:00+ UTC). Etc/GMT-14 was ambiguous in some pytz
    # versions and caused Vertica to return wrong results.
    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "sla_time": "12:01am",
        "timezone": "UTC",
    }
    test_result = dbt_project.test(test_id, TEST_NAME, test_args, data=data)
    assert test_result["status"] == "fail"


def test_no_data_fails(test_id: str, dbt_project: DbtProject):
    """An empty table (after WHERE filter) should fail when deadline has passed."""
    utc_now = datetime.utcnow()
    # Seed with data that will be excluded by the where_expression
    data = [
        {TIMESTAMP_COLUMN: utc_now.strftime(DATE_FORMAT), "category": "excluded"},
    ]
    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "sla_time": "12:01am",
        "timezone": "UTC",
        "where_expression": "category = 'included'",
    }
    test_result = dbt_project.test(test_id, TEST_NAME, test_args, data=data)
    assert test_result["status"] == "fail"


def test_deadline_not_passed_does_not_fail(test_id: str, dbt_project: DbtProject):
    """Even if data is stale, the test should pass if the deadline hasn't passed yet."""
    utc_now = datetime.utcnow()
    yesterday = utc_now - timedelta(days=2)
    data = [
        {TIMESTAMP_COLUMN: yesterday.strftime(DATE_FORMAT)},
    ]
    # Set the deadline to 11:59pm UTC so it reliably hasn't passed yet.
    # (Etc/GMT-14 = UTC+14 means 11:59pm there = 09:59 UTC — not reliably future)
    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "sla_time": "11:59pm",
        "timezone": "UTC",
    }
    test_result = dbt_project.test(test_id, TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"


def test_with_where_expression(test_id: str, dbt_project: DbtProject):
    """The where_expression should filter which rows count toward freshness."""
    utc_now = datetime.utcnow()
    yesterday = utc_now - timedelta(days=2)
    data = [
        # Fresh data for category A
        {TIMESTAMP_COLUMN: utc_now.strftime(DATE_FORMAT), "category": "a"},
        # Stale data for category B
        {TIMESTAMP_COLUMN: yesterday.strftime(DATE_FORMAT), "category": "b"},
    ]
    # Test with category A (fresh data) -> should pass
    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "sla_time": "11:59pm",
        "timezone": "UTC",
        "where_expression": "category = 'a'",
    }
    test_result = dbt_project.test(test_id, TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"

    # Test with category B (stale data) and early deadline -> should fail
    test_args_stale = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "sla_time": "12:01am",
        "timezone": "UTC",
        "where_expression": "category = 'b'",
    }
    test_result = dbt_project.test(test_id, TEST_NAME, test_args_stale)
    assert test_result["status"] == "fail"


def test_with_timezone(test_id: str, dbt_project: DbtProject):
    """Test that timezone conversion works correctly."""
    utc_now = datetime.utcnow()
    data = [
        {TIMESTAMP_COLUMN: utc_now.strftime(DATE_FORMAT)},
    ]
    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "sla_time": "11:59pm",
        "timezone": "America/New_York",
    }
    test_result = dbt_project.test(test_id, TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"
