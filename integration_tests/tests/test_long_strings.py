import pytest
from dbt_project import DbtProject


@pytest.mark.skip_targets(["postgres"])
def test_long_string(test_id: str, dbt_project: DbtProject):
    dbt_project.dbt_runner.vars["disable_run_results"] = False
    column_size = int(
        dbt_project.dbt_runner.run_operation("elementary.get_column_size")[0]
    )
    query_start = "SELECT '"
    query_end = "' as col"
    query_mid = "A" * (column_size - len(query_start) - len(query_end))
    query = query_start + query_mid + query_end
    with dbt_project.create_temp_model_for_existing_table(
        test_id, "table", query
    ) as model_path:
        dbt_project.dbt_runner.run(select=str(model_path))
    result = dbt_project.read_table(
        "dbt_run_results",
        "unique_id = 'model.elementary_tests.test_long_string'",
    )[0]
    assert len(result["compiled_code"]) == column_size
