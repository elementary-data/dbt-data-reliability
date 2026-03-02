{% macro get_latest_full_refresh(model_node) %}
    {%- set dbt_run_results_relation = elementary.get_elementary_relation('dbt_run_results') %}
    {% set query %}
        select generated_at from {{ dbt_run_results_relation }}
        where
          unique_id = '{{ model_node.unique_id }}' and
          full_refresh = true
        order by generated_at desc
        limit 1
    {% endset %}
    {% do return(elementary.result_value(query)) %}
{% endmacro %}
