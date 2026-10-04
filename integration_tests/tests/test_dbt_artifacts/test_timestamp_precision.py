import json
import uuid

import pytest
from dbt_project import DbtProject

NANOSECOND_TIMESTAMP = "2026-04-03T10:50:50.961498756Z"
MICROSECOND_TIMESTAMP = "2026-04-03T10:50:50.961498Z"


@pytest.mark.parametrize(
    "input_value,expected_output",
    [
        (NANOSECOND_TIMESTAMP, MICROSECOND_TIMESTAMP),
        ("2026-04-03T10:50:50.9614987Z", MICROSECOND_TIMESTAMP),
        ("2026-04-03 10:50:50.961498756+00:00", "2026-04-03 10:50:50.961498+00:00"),
        (MICROSECOND_TIMESTAMP, MICROSECOND_TIMESTAMP),
        ("2026-04-03T10:50:50.961Z", "2026-04-03T10:50:50.961Z"),
        ("2026-04-03T10:50:50Z", "2026-04-03T10:50:50Z"),
        (None, None),
    ],
)
def test_truncate_timestamp_precision(
    dbt_project: DbtProject, input_value, expected_output
):
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_truncate_timestamp_precision",
        macro_args={"value": input_value},
    )
    # When the macro returns None, log_macro_results doesn't log anything
    actual_output = json.loads(result[0]) if result else None
    assert actual_output == expected_output


@pytest.mark.only_on_targets(["bigquery"])
def test_bigquery_cast_truncates_nanoseconds_by_default(dbt_project: DbtProject):
    rendered = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_render_cast_as_timestamp",
        macro_args={"column_name": "updated_at"},
    )
    assert "regexp_replace" in json.loads(rendered[0]).lower()

    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_cast_as_timestamp",
        macro_args={"value": NANOSECOND_TIMESTAMP},
    )
    assert json.loads(result[0]).startswith("2026-04-03T10:50:50.961498")


# The string round trip prevents BigQuery partition pruning,
# so disabling the var must give a plain cast.
@pytest.mark.only_on_targets(["bigquery"])
def test_bigquery_cast_is_plain_when_truncation_disabled(dbt_project: DbtProject):
    rendered = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_render_cast_as_timestamp",
        macro_args={"column_name": "updated_at"},
        vars={"bigquery_truncate_nanosecond_timestamps": False},
    )
    assert "regexp_replace" not in json.loads(rendered[0]).lower()


@pytest.mark.only_on_targets(["bigquery"])
def test_fix_nanosecond_timing_values(dbt_project: DbtProject):
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_fix_nanosecond_timing_values",
        macro_args={
            "model_execution_id": f"test_fix_nanoseconds_{uuid.uuid4().hex}",
            "value": NANOSECOND_TIMESTAMP,
        },
    )
    assert json.loads(result[-1]) == [MICROSECOND_TIMESTAMP, MICROSECOND_TIMESTAMP]
