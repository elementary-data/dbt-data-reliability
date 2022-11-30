{% macro result_column_to_list(single_column_query) %}
    {% set column_values = [] %}
    {% set query_result = dbt.run_query(single_column_query) %}
    {% set results_rows = query_result.rows.values() %}
    {% for result_row in results_rows %}
        {% do column_values.append(results_rows[loop.index0].values()[0]) %}
    {% endfor %}
    {% do return(column_values) %}
{% endmacro %}
