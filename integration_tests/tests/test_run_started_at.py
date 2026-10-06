from datetime import datetime, time, timedelta
from typing import Any, Dict, List
from unittest import mock

import pytest
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject
from parametrization import Parametrization

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.volume_anomalies"
DBT_TEST_ARGS = {"timestamp_column": TIMESTAMP_COLUMN}
DATA = [{TIMESTAMP_COLUMN: "2026-01-01 00:00:00"}]


def _freeze_harness_clock(monkeypatch, now: datetime) -> mock.Mock:
    """Make DbtProject.test read `now` as the UTC time. dbt keeps the real clock."""
    clock = mock.Mock(wraps=datetime)
    clock.utcnow.return_value = now
    monkeypatch.setattr("dbt_project.datetime", clock)
    return clock


@pytest.fixture
def dbt_test_vars(monkeypatch, dbt_project: DbtProject) -> List[Dict[str, Any]]:
    """Stub seeding, dbt and result reading, and collect the vars of each dbt call."""
    calls: List[Dict[str, Any]] = []

    def fake_dbt_test(**kwargs):
        calls.append(kwargs["vars"])
        return True

    monkeypatch.setattr(dbt_project, "seed", lambda data, table_name: None)
    monkeypatch.setattr(dbt_project.dbt_runner, "test", fake_dbt_test)
    monkeypatch.setattr(
        dbt_project, "_read_single_test_result", lambda test_id: {"status": "pass"}
    )
    return calls


def test_run_started_at_is_taken_before_seeding(
    test_id: str, dbt_project: DbtProject, monkeypatch, dbt_test_vars
):
    before_midnight = datetime(2026, 1, 1, 23, 59, 59)
    clock = _freeze_harness_clock(monkeypatch, before_midnight)

    def seed_past_midnight(data, table_name):
        clock.utcnow.return_value = before_midnight + timedelta(minutes=5)

    monkeypatch.setattr(dbt_project, "seed", seed_past_midnight)

    dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=DATA)

    assert [v["custom_run_started_at"] for v in dbt_test_vars] == [
        before_midnight.isoformat()
    ]


def test_run_started_at_from_caller_is_kept(
    test_id: str, dbt_project: DbtProject, monkeypatch, dbt_test_vars
):
    _freeze_harness_clock(monkeypatch, datetime(2026, 1, 2))
    caller_run_started_at = datetime(2000, 1, 1).isoformat()

    dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=DATA,
        test_vars={"custom_run_started_at": caller_run_started_at},
    )

    assert [v["custom_run_started_at"] for v in dbt_test_vars] == [
        caller_run_started_at
    ]


def test_run_started_at_is_taken_per_call(
    test_id: str, dbt_project: DbtProject, monkeypatch, dbt_test_vars
):
    first_call_at = datetime(2026, 1, 1, 12)
    second_call_at = first_call_at + timedelta(hours=1)
    clock = _freeze_harness_clock(monkeypatch, first_call_at)
    # One dict reused across calls must not freeze the time of the first call.
    # Not empty, since `{} or {}` would hand the harness a new dict anyway.
    test_vars: Dict[str, Any] = {"debug_logs": True}

    dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=DATA, test_vars=test_vars
    )
    clock.utcnow.return_value = second_call_at
    dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=DATA, test_vars=test_vars
    )

    assert [v["custom_run_started_at"] for v in dbt_test_vars] == [
        first_call_at.isoformat(),
        second_call_at.isoformat(),
    ]
    assert test_vars == {"debug_logs": True}


# Runs dbt for real. The harness clock reads one minute before the last UTC
# midnight, while dbt runs on the real clock, a day later: the same as a CI run
# where midnight passes while seeding. The buckets must follow the harness clock.
# The seeded table is named after the test id: Postgres allows 63 characters.
@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="unfinished_bucket", spike_days_ago=0, expected_status="pass"
)
@Parametrization.case(name="last_full_bucket", spike_days_ago=1, expected_status="fail")
def test_buckets_follow_harness_clock(
    test_id: str,
    dbt_project: DbtProject,
    monkeypatch,
    spike_days_ago: int,
    expected_status: str,
):
    last_midnight = datetime.combine(datetime.utcnow().date(), time.min)
    harness_now = last_midnight - timedelta(minutes=1)
    _freeze_harness_clock(monkeypatch, harness_now)

    harness_today = harness_now.date()
    spike_date = harness_today - timedelta(days=spike_days_ago)
    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=harness_today)
    ]
    data += [{TIMESTAMP_COLUMN: spike_date.strftime(DATE_FORMAT)}] * 10

    test_args = {**DBT_TEST_ARGS, "backfill_days": 1}
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == expected_status
