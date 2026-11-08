import json

import pytest
from dbt_project import DbtProject

ENTRIES = {
    "elementary_test_results",
    "elementary_test_failed_row_counts",
    "tests_schema_name",
    "tables",
    "temp_test_table_relations_map",
    "duration_context_stack",
    "microbatch_compiled_code_by_unique_id",
}


@pytest.mark.parametrize(
    "existing",
    [
        pytest.param({}, id="empty_entry_left_by_an_earlier_read"),
        pytest.param({"tests_schema_name": "kept"}, id="entry_with_a_value"),
    ],
)
def test_init_elementary_graph_fills_every_missing_entry(
    dbt_project: DbtProject, existing: dict
):
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_init_elementary_graph",
        macro_args={"existing": existing},
    )
    elementary_graph = json.loads(result[0])
    assert set(elementary_graph) == ENTRIES
    assert elementary_graph["tables"] == {
        "metrics": {"relations": [], "rows": []},
        "schema_snapshots": [],
    }
    for entry, value in existing.items():
        assert elementary_graph[entry] == value
