{% macro get_model_sql_from_node(node) %}
    {% set should_collect_model_sql = elementary.get_config_var('collect_model_sql') %}
    {% set model_sql_max_size = elementary.get_config_var('model_sql_max_size') %}
    {% set long_string_size = elementary.get_config_var('long_string_size') %}
    {% set model_sql_size_limit = [model_sql_max_size, long_string_size] | min %}
    {% set model_sql = elementary.insensitive_get_dict_value(node, 'compiled_sql', '') %}
    {% if not should_collect_model_sql %}
        {{ return(none) }}
    {% elif model_sql_size_limit < model_sql | length %}
        {{ return('Query is too long - over ' ~ model_sql_size_limit ~ ' bytes') }}
    {% else %}
        {{ return(model_sql) }}
    {% endif %}
{% endmacro %}
