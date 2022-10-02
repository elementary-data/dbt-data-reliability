{% macro get_compiled_code_text(node) %}
    {% set model_sql_max_size = elementary.get_config_var('model_sql_max_size') %}
    {% set long_string_size = elementary.get_config_var('long_string_size') %}
    {% set model_sql_size_limit = [model_sql_max_size, long_string_size] | min %}
    {% set model_code = elementary.get_compiled_code(node) %}
    {% if not model_code %}
        {{ return(none) }}
    {% endif %}
    {% if model_sql_size_limit < model_code | length %}
        {{ return('Code is too long - over ' ~ model_sql_size_limit ~ ' bytes') }}
    {% else %}
        {{ return(model_code ) }}
    {% endif %}
{% endmacro %}
