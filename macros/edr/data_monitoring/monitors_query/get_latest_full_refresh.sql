{% macro get_latest_full_refresh(model_node) %}
    {% set query %}
        select generated_at from {{ ref('dbt_run_results') }}
        where
          unique_id = '{{ model_node.unique_id }}' and
          full_refresh = true
        {{ elementary.orderby('generated_at desc') }}
    {% endset %}
    {% do return(elementary.result_value(elementary.limit(query))) %}
{% endmacro %}
