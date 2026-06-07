"""Integration test for elementary.cleanup_stale_test_tables macro."""

import json

import pytest
from dbt_project import DbtProject


# Spark: no information_schema in Hive metastore, no LIKE-pattern catalog API.
@pytest.mark.skip_targets(["spark"])
def test_cleanup_stale_test_tables(dbt_project: DbtProject):
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_cleanup_stale_test_tables",
    )
    assert result, "run_operation returned no output"
    data = json.loads(result[0])

    assert (
        data["tables_before_count"] >= 2
    ), f"Expected at least 2 temp tables before cleanup, got {data['tables_before_count']}"
    assert (
        data["tables_after_count"] == 0
    ), f"Expected 0 temp tables after cleanup, got {data['tables_after_count']}"
