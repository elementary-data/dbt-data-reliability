{% macro get_compiled_sql_from_node(node) %}
    {% set should_collect_compiled_sql = elementary.get_config_var('collect_compiled_sql') %}
    {% set compiled_sql_max_size = elementary.get_config_var('long_string_size') %}
    {% set compiled_sql = elementary.insensitive_get_dict_value(node, 'compiled_sql', '') %}
    {% if not should_collect_compiled_sql %}
        {{ return(none) }}
    {% elif compiled_sql_max_size < compiled_sql | length %}
        {{ return('Query is too long - over ' ~ compiled_sql_max_size ~ ' bytes') }}
    {% else %}
        {{ return(compiled_sql) }}
    {% endif %}
{% endmacro %}
