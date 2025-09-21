import pytest
from dbt_project import DbtProject

SAFE_QUERY_SIZE = 10000


def generate_query(query_size: int) -> str:
    query_start = "SELECT '"
    query_end = "' as col"
    query_mid = "A" * (query_size - len(query_start) - len(query_end))
    return query_start + query_mid + query_end


def read_run_result(dbt_project, test_id):
    return dbt_project.read_table(
        "dbt_run_results",
        where=f"unique_id = 'model.elementary_tests.{test_id}'",
    )[0]


@pytest.mark.skip_for_dbt_fusion
def test_query_size_exceed(test_id: str, dbt_project: DbtProject):
    dbt_project.dbt_runner.vars["disable_run_results"] = False
    max_query_size = int(
        dbt_project.dbt_runner.run_operation(
            "elementary.get_config_var", macro_args={"var_name": "query_max_size"}
        )[0]
    )

    query = generate_query(max_query_size)
    with dbt_project.create_temp_model_for_existing_table(
        test_id, raw_code=query
    ) as model_path:
        dbt_project.dbt_runner.run(select=str(model_path))
        result = read_run_result(dbt_project, test_id)
        # Expect truncation.
        assert len(result["compiled_code"]) < max_query_size


@pytest.mark.skip_for_dbt_fusion
def test_query_size_safe(test_id: str, dbt_project: DbtProject):
    dbt_project.dbt_runner.vars["disable_run_results"] = False
    query = generate_query(SAFE_QUERY_SIZE)
    with dbt_project.create_temp_model_for_existing_table(
        test_id, raw_code=query
    ) as model_path:
        dbt_project.dbt_runner.run(select=str(model_path))
        result = read_run_result(dbt_project, test_id)
        assert len(result["compiled_code"]) == SAFE_QUERY_SIZE
