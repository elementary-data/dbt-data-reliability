"""Renders bigquery__edr_get_create_table_as_sql and checks the expiration clause.

BigQuery is the only adapter whose edr_get_create_table_as_sql emits
expiration_timestamp, so this is the only target where the assertions mean
anything.
"""

import json

import pytest
from dbt_project import DbtProject


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


@pytest.mark.only_on_targets(["bigquery"])
def test_temp_table_expiration(dbt_project: DbtProject):
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_temp_table_expiration",
    )
    assert result, "run_operation returned no output"
    cases = {key: _normalize(sql) for key, sql in json.loads(result[0]).items()}

    # Default for a temp table comes from temp_table_expiration_hours (default 1).
    assert "INTERVAL 1 hour" in cases["temp_default"]

    # An explicit expiration_hours used to be swallowed by the temporary branch.
    assert "INTERVAL 6 hour" in cases["temp_explicit"]
    assert "INTERVAL 1 hour" not in cases["temp_explicit"]

    # Non-temp tables get no expiration unless one is asked for. The test tables
    # created by create_elementary_test_table take this second path.
    assert "expiration_timestamp" not in cases["non_temp_default"]
    assert "INTERVAL 6 hour" in cases["non_temp_explicit"]
