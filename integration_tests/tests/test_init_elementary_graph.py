import json
from typing import Optional

import pytest
from dbt_project import DbtProject

DEFAULTS = {
    "elementary_test_results": {},
    "elementary_test_failed_row_counts": {},
    "tests_schema_name": None,
    "tables": {
        "metrics": {"relations": [], "rows": []},
        "schema_snapshots": [],
    },
    "temp_test_table_relations_map": {},
    "duration_context_stack": {},
    "microbatch_compiled_code_by_unique_id": {},
}


@pytest.mark.parametrize(
    "existing",
    [
        pytest.param(None, id="no_entry_yet"),
        pytest.param({}, id="empty_entry_left_by_an_earlier_read"),
        pytest.param({"tests_schema_name": "kept"}, id="entry_with_a_value"),
        pytest.param(
            {"elementary_test_results": {"test.pkg.not_null_id": [{"status": "pass"}]}},
            id="cache_entry_with_results",
        ),
    ],
)
def test_init_elementary_graph_fills_every_missing_entry(
    dbt_project: DbtProject, existing: Optional[dict]
):
    """init_elementary_graph fills each missing entry and keeps the ones present.

    `existing` is what an earlier read can leave under graph["elementary"] (None: no
    entry yet, as on dbt v1): dbt v2
    shares `graph` between parsing and the run, so a get_cache rendered at parse
    time leaves an empty mapping before the on-run-start hook runs.
    """
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_init_elementary_graph",
        macro_args={"existing": existing},
    )
    elementary_graph = json.loads(result[0])
    assert elementary_graph == {**DEFAULTS, **(existing or {})}
