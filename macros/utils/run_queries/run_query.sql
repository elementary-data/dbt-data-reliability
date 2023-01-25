{% macro run_query(query) %}
    {% set query_result = dbt.run_query(query) %}
    {% set lowercased_column_names = {} %}
    {% for column_name in query_result.column_names %}
        {% do lowercased_column_names.update({column_name: column_name.lower()}) %}
    {% endfor %}
    {% do return(query_result.rename(lowercased_column_names)) %}
{% endmacro %}
