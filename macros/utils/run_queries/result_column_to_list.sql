{% macro result_column_to_list(single_column_query) %}
    {% set column_values = [] %}
    {% set query_result = dbt.run_query(single_column_query) %}
    {% do return(query_result.columns[0]) %}
{% endmacro %}
