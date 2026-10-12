{% macro insert_run_result_row(model_execution_id, execution_time) %}
    {#- Insert a dbt_run_results row through elementary.insert_rows, the same
        path the on-run-end upload uses, so tests can control the exact
        execution_time value that gets rendered and stored. -#}
    {% do elementary.insert_rows(
        ref("dbt_run_results"),
        [
            {
                "model_execution_id": model_execution_id,
                "unique_id": "model.elementary_tests.precision_sentinel",
                "name": "precision_sentinel",
                "execution_time": execution_time,
            }
        ],
        should_commit=true,
    ) %}
{% endmacro %}
