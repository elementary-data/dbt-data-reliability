{% macro dbt_model_run_result_description() %}
    'The model ' || name || ' returned ' || status || ' at ' || generated_at || ' on run ' || invocation_id
{% endmacro %}