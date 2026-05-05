from contextlib import contextmanager

import pytest

from dbt_project import DbtProject


def _microbatch_source_model_sql() -> str:
    return """
{{ config(event_time='order_date') }}
{% set event_time_data_type = 'datetime2' if target.type == 'sqlserver' else 'timestamp' %}

select
    1 as order_id,
    1 as customer_id,
    42 as amount,
    cast('2024-01-01 00:00:00' as {{ event_time_data_type }}) as order_date
union all
select
    2 as order_id,
    2 as customer_id,
    84 as amount,
    cast('2025-01-01 00:00:00' as {{ event_time_data_type }}) as order_date
"""


def _microbatch_model_sql(source_model_name: str) -> str:
    return """
{% set model_config = {
    "materialized": "incremental",
    "incremental_strategy": "microbatch",
    "event_time": "order_date",
    "batch_size": "year",
    "begin": "2024-01-01"
} %}
{% if target.type != "duckdb" %}
    {% do model_config.update({"unique_key": "order_id"}) %}
{% endif %}
{{ config(**model_config) }}

select
    order_id,
    customer_id,
    amount,
    order_date
from {{ ref('__MICROBATCH_SOURCE_MODEL__') }}
""".replace("__MICROBATCH_SOURCE_MODEL__", source_model_name)


@contextmanager
def _with_microbatch_test_models(dbt_project: DbtProject, model_suffix: str):
    source_model_name = f"mb_src_{model_suffix}"
    target_model_name = f"mb_tgt_{model_suffix}"
    source_model_path = dbt_project.tmp_models_dir_path.joinpath(f"{source_model_name}.sql")
    target_model_path = dbt_project.tmp_models_dir_path.joinpath(f"{target_model_name}.sql")

    source_model_path.write_text(_microbatch_source_model_sql())
    target_model_path.write_text(_microbatch_model_sql(source_model_name))
    relative_source_model_path = source_model_path.relative_to(dbt_project.project_dir_path)
    relative_target_model_path = target_model_path.relative_to(dbt_project.project_dir_path)
    try:
        yield relative_source_model_path, relative_target_model_path, target_model_name
    finally:
        if source_model_path.exists():
            source_model_path.unlink()
        if target_model_path.exists():
            target_model_path.unlink()


def _run_microbatch_model_and_get_latest_success_result(
    dbt_project: DbtProject, model_suffix: str
):
    with _with_microbatch_test_models(dbt_project, model_suffix) as (
        source_model_path,
        model_path,
        target_model_name,
    ):
        dbt_project.dbt_runner.run(
            select=f"{source_model_path} {model_path}"
        )

    unique_id = f"model.elementary_tests.{target_model_name}"
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
    macro_sql = """
{% macro __MACRO_NAME__(arg_dict) %}
  {{ return(elementary.get_incremental_microbatch_sql(arg_dict)) }}
{% endmacro %}
""".replace("__MACRO_NAME__", macro_name)
    if macro_path.exists():
        raise FileExistsError(f"Expected no macro file at {macro_path}")

    macro_path.write_text(macro_sql)
    try:
        yield
    finally:
        if macro_path.exists():
            macro_path.unlink()


@pytest.mark.skip_targets(["spark", "vertica", "bigquery", "athena", "clickhouse", "dremio"])
@pytest.mark.skip_for_dbt_fusion
@pytest.mark.parametrize(
    "macro_name,expected_compiled_code,model_suffix",
    [
        ("get_incremental_microbatch_sql", True, "with_override"),
        ("get_incremental_microbatch_sql_not_used", False, "without_override"),
    ],
    ids=["with_override", "without_override"],
)
def test_microbatch_run_results_compiled_code_behavior(
    dbt_project: DbtProject,
    macro_name: str,
    expected_compiled_code: bool,
    model_suffix: str,
):
    dbt_project.dbt_runner.vars["disable_run_results"] = False

    with _with_microbatch_macro_file(dbt_project, macro_name):
        run_results = _run_microbatch_model_and_get_latest_success_result(
            dbt_project, model_suffix
        )
    assert run_results, "Expected a successful run result row for microbatch model"
    if expected_compiled_code:
        assert run_results[0]["compiled_code"], (
            "Expected compiled_code to be populated when override macro is present"
        )
    else:
        assert not run_results[0]["compiled_code"], (
            "Expected compiled_code to stay empty when override macro is absent"
        )
