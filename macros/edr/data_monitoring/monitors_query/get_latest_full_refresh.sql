{% macro get_latest_full_refresh(model_node) %}
    {% set query %}
        select generated_at from {{ ref('dbt_run_results') }}
        where
          unique_id = '{{ model_node.unique_id }}' and
          full_refresh = true
        order by generated_at desc
        limit 1
    {% endset %}
    {% do return(elementary.result_value(query)) %}
{% endmacro %}
