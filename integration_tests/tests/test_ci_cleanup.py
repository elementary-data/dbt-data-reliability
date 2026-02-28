"""Integration test for elementary.drop_stale_ci_schemas macro."""

import json

import pytest
from dbt_project import DbtProject


@pytest.mark.skip_targets(["dremio"])
def test_drop_stale_ci_schemas(dbt_project: DbtProject):
    """Verify that old CI schemas are dropped and recent ones are kept."""
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_drop_stale_ci_schemas",
    )
    assert result, "run_operation returned no output"
    data = json.loads(result[0])

    assert data["old_exists_before"], "Setup failed: old schema was not created"
    assert data["recent_exists_before"], "Setup failed: recent schema was not created"
    assert data["old_dropped"], "Old schema should have been dropped by cleanup"
    assert data["recent_kept"], "Recent schema should have been kept by cleanup"
