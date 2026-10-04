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
@pytest.mark.parametrize(
    "table_name,id_column",
    [
        ("dbt_run_results", "model_execution_id"),
        ("dbt_source_freshness_results", "source_freshness_execution_id"),
    ],
)
def test_fix_nanosecond_timing_values(
    dbt_project: DbtProject, table_name: str, id_column: str
):
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_fix_nanosecond_timing_values",
        macro_args={
            "table_name": table_name,
            "id_column": id_column,
            "row_id": f"test_fix_nanoseconds_{uuid.uuid4().hex}",
            "value": NANOSECOND_TIMESTAMP,
        },
    )
    timing_values = json.loads(result[-1])
    # Before the fix the table still holds the 9-digit values, after it they're truncated.
    assert timing_values["before"] == [NANOSECOND_TIMESTAMP] * 4
    assert timing_values["after"] == [MICROSECOND_TIMESTAMP] * 4
