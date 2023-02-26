{% macro result_column_to_list(single_column_query) %}
    {% set query_result = elementary.run_query(single_column_query) %}
    {% do return(query_result.columns[0]) %}
{% endmacro %}
