{% macro run_query(query, lowercase_column_names=True) %}
    {% set query_result = dbt.run_query(query) %}
    {% if lowercase_column_names %}
        {% set lowercased_column_names = {} %}
        {% for column_name in query_result.column_names %}
            {% do lowercased_column_names.update({column_name: column_name.lower()}) %}
        {% endfor %}
        {% set query_result = query_result.rename(lowercased_column_names) %}
    {% endif %}

    {% do return(query_result) %}
{% endmacro %}
