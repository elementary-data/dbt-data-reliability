from contextlib import contextmanager

import pytest

from dbt_project import DbtProject


def _microbatch_model_sql() -> str:
    return """
{% set model_config = {
    "materialized": "incremental",
    "incremental_strategy": "microbatch",
    "event_time": "order_date",
    "batch_size": "year",
    "begin": "2025-03-01",
    "unique_key": "order_id"
} %}
{% if target.type == "bigquery" %}
    {% do model_config.update(
        {"partition_by": {"field": "order_date", "data_type": "timestamp", "granularity": "year"}}
    ) %}
{% endif %}
{% if target.type == "athena" %}
    {% do model_config.update({"partitioned_by": ["order_date"]}) %}
{% endif %}
{{ config(**model_config) }}

select
    1 as order_id,
    1 as customer_id,
    42 as amount,
    {{ dbt.current_timestamp() }} as order_date
from {{ ref('one') }}
"""


def _run_microbatch_model_and_get_latest_success_result(
    dbt_project: DbtProject, test_id: str
):
    with dbt_project.create_temp_model_for_existing_table(
        test_id, raw_code=_microbatch_model_sql()
    ) as model_path:
        dbt_project.dbt_runner.run(select=str(model_path))

    unique_id = f"model.elementary_tests.{test_id}"
    run_results = dbt_project.read_table(
        "dbt_run_results",
        where=f"unique_id = '{unique_id}' and status = 'success'",
        order_by="generated_at desc",
        limit=1,
    )
    return run_results


@contextmanager
def _with_microbatch_macro_file(dbt_project: DbtProject, macro_name: str):
    macro_path = (
        dbt_project.project_dir_path / "macros" / "microbatch.sql"
    )
    macro_sql = f"""{{% macro {macro_name}(arg_dict) %}}
  {{ return(elementary.get_incremental_microbatch_sql(arg_dict)) }}
{{% endmacro %}}
"""
    if macro_path.exists():
        raise FileExistsError(f"Expected no macro file at {macro_path}")

    macro_path.write_text(macro_sql)
    try:
        yield
    finally:
        if macro_path.exists():
            macro_path.unlink()


@pytest.mark.skip_targets(["vertica"])
@pytest.mark.skip_for_dbt_fusion
def test_microbatch_run_results_has_compiled_code(test_id: str, dbt_project: DbtProject):
    dbt_project.dbt_runner.vars["disable_run_results"] = False

    with _with_microbatch_macro_file(dbt_project, "get_incremental_microbatch_sql"):
        run_results = _run_microbatch_model_and_get_latest_success_result(
            dbt_project, test_id
        )
    assert run_results, "Expected a successful run result row for microbatch model"
    assert run_results[0]["compiled_code"], (
        "Expected compiled_code to be populated for successful microbatch model run result"
    )


@pytest.mark.skip_targets(["vertica"])
@pytest.mark.skip_for_dbt_fusion
def test_microbatch_run_results_without_override_has_empty_compiled_code(
    test_id: str, dbt_project: DbtProject
):
    dbt_project.dbt_runner.vars["disable_run_results"] = False

    with _with_microbatch_macro_file(
        dbt_project, "get_incremental_microbatch_sql_not_used"
    ):
        run_results = _run_microbatch_model_and_get_latest_success_result(
            dbt_project, test_id
        )
    assert run_results, "Expected a successful run result row for microbatch model"
    assert not run_results[0]["compiled_code"], (
        "Expected compiled_code to stay empty when microbatch override macro is absent"
    )
