{% macro get_compiled_model_code_text(node) %}
    {% set should_collect_model_sql = elementary.get_config_var('collect_model_sql') %}
    {% if not should_collect_model_sql %}
        {{ return(none) }}
    {% endif %}

    {% set model_sql_max_size = elementary.get_config_var('model_sql_max_size') %}
    {% set long_string_size = elementary.get_config_var('long_string_size') %}
    {% set model_sql_size_limit = [model_sql_max_size, long_string_size] | min %}
    {% set model_code = elementary.get_compiled_code(node) %}

    {# Seeds do not have compiled code. #}
    {% if not model_code %}
        {{ return(none) }}
    {% endif %}

    {% if model_sql_size_limit < model_code | length %}
        {{ return('Model code is too long - over ' ~ model_sql_size_limit ~ ' bytes') }}
    {% else %}
        {{ return(model_code) }}
    {% endif %}
{% endmacro %}
