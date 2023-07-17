{% macro get_row_count(full_table_name) %}

    {% set query_row_count %}
        select count(*) from {{ full_table_name }}
    {% endset %}
    {% if execute %}
        {% set result = elementary.run_query(query_row_count).columns[0].values()[0] %}
    {% endif %}
    {{ return(result) }}

{% endmacro %}}