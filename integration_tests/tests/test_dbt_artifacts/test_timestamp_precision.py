import json

import pytest
from dbt_project import DbtProject


@pytest.mark.parametrize(
    "input_value,expected_output",
    [
        ("2026-04-03T10:50:50.961498756Z", "2026-04-03T10:50:50.961498Z"),
        ("2026-04-03T10:50:50.9614987Z", "2026-04-03T10:50:50.961498Z"),
        ("2026-04-03 10:50:50.961498756+00:00", "2026-04-03 10:50:50.961498+00:00"),
        ("2026-04-03T10:50:50.961498Z", "2026-04-03T10:50:50.961498Z"),
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


# The string round trip in the metadata cast prevents BigQuery partition pruning,
# so it must never leak into the generic cast used on monitored tables.
@pytest.mark.only_on_targets(["bigquery"])
def test_bigquery_timestamp_cast_keeps_partition_pruning(dbt_project: DbtProject):
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_render_timestamp_casts",
        macro_args={"column_name": "updated_at"},
    )
    casts = json.loads(result[0])
    assert "regexp_replace" not in casts["cast_as_timestamp"].lower()
    assert "regexp_replace" in casts["cast_metadata_timestamp"].lower()


@pytest.mark.only_on_targets(["bigquery"])
def test_bigquery_cast_metadata_timestamp_nanoseconds(dbt_project: DbtProject):
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_cast_metadata_timestamp",
        macro_args={"value": "2026-04-03T10:50:50.961498756Z"},
    )
    assert json.loads(result[0]).startswith("2026-04-03T10:50:50.961498")
