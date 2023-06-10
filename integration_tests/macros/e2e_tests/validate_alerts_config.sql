{% macro validate_alerts_config() %}
    {% set dbt_models_relation = ref('dbt_models') %}
    {% set dbt_models_query %}
        select distinct name, {{ elementary.get_json_path('meta','alerts_config') }} as alerts_config
        from {{ dbt_models_relation }}
    {% endset %}
    {%- set results_agate = elementary.run_query(dbt_models_query) %}
    {%- set results_dict = elementary.agate_to_dicts(results_agate) %}
    {%- do print(results_dict) -%}
{% endmacro %}