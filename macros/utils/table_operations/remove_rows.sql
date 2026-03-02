{% macro remove_rows(table_name) %}

    {% set remove_rows_query %}
        delete from {{ table_name }}
    {% endset %}
    {% do elementary.run_query(remove_rows_query) %}

{% endmacro %}