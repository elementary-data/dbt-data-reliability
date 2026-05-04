import pytest

from dbt_project import DbtProject


@pytest.mark.skip_targets(["vertica"])
@pytest.mark.skip_for_dbt_fusion
def test_microbatch_run_results_has_compiled_code(test_id: str, dbt_project: DbtProject):
    dbt_project.dbt_runner.vars["disable_run_results"] = False

    model_sql = """
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
    cast({{ elementary.escape_reserved_keywords("one") }} as int) as order_id,
    1 as customer_id,
    42 as amount,
    {{ dbt.current_timestamp() }} as order_date
from {{ ref('one') }}
"""

    with dbt_project.create_temp_model_for_existing_table(
        test_id, raw_code=model_sql
    ) as model_path:
        dbt_project.dbt_runner.run(select=str(model_path))

    unique_id = f"model.elementary_tests.{test_id}"
    run_results = dbt_project.read_table(
        "dbt_run_results",
        where=f"unique_id = '{unique_id}' and status = 'success'",
        order_by="generated_at desc",
        limit=1,
    )
    assert run_results, "Expected a successful run result row for microbatch model"
    assert run_results[0]["compiled_code"], (
        "Expected compiled_code to be populated for successful microbatch model run result"
    )
