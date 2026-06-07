"""Integration test for elementary.cleanup_stale_test_tables macro."""

import json

import pytest
from dbt_project import DbtProject


# Spark: no information_schema in Hive metastore, no LIKE-pattern catalog API.
# Dremio: INFORMATION_SCHEMA.table_schema stores the full dot-separated space path
# (e.g. "space.folder.schema"), so matching on schema name alone returns no rows.
@pytest.mark.skip_targets(["spark", "dremio"])
def test_cleanup_stale_test_tables(dbt_project: DbtProject):
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_cleanup_stale_test_tables",
    )
    assert result, "run_operation returned no output"
    data = json.loads(result[0])

    assert (
        data["tables_before_count"] >= 3
    ), f"Expected at least 3 temp tables before cleanup, got {data['tables_before_count']}"
    assert (
        data["tables_after_count"] == 1
    ), f"Expected 1 temp table after cleanup (limit=2), got {data['tables_after_count']}"
