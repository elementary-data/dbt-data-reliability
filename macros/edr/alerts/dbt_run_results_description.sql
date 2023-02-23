{% macro dbt_model_run_result_description() -%}
    {{ return(adapter.dispatch('freshness_description', 'elementary') ()) }}
{%- endmacro %}

{% macro default__dbt_model_run_result_description() %}
    'The model ' || name || ' returned ' || status || ' at ' || generated_at || ' on run ' || invocation_id
{% endmacro %}

{% macro sqlserver__dbt_model_run_result_description() %}
    'The model ' + name + ' returned ' + status + ' at ' + generated_at + ' on run ' + invocation_id
{% endmacro %}
