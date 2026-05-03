from dbt_project import DbtProject


def test_microbatch_run_results_has_compiled_code(test_id: str, dbt_project: DbtProject):
    dbt_project.dbt_runner.vars["disable_run_results"] = False

    model_sql = """
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    event_time='order_date',
    batch_size='day',
    begin='2025-03-01',
    unique_key='order_id'
) }}

select
    order_id,
    customer_id,
    amount,
    cast('2025-03-01 00:00:00+00:00' as timestamp) as order_date
from {{ ref('stg_orders') }}
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
