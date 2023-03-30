
{% macro assert_empty_table(table, context='') %}
    {% if table | length > 0 %}
        {% do elementary.edr_log(context ~ " FAILED: Table not empty.") %}
        {% do table.print_table() %}
        {{ return(1) }}
    {% endif %}
    {% do elementary.edr_log(context ~ " SUCCESS: Table is empty.") %}
    {{ return(0) }}
{% endmacro %}
