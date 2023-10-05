{% macro get_compiled_model_code_text(node) %}
    {% set should_collect_model_sql = elementary.get_config_var('collect_model_sql') %}
    {% if not should_collect_model_sql %}
        {{ return(none) }}
    {% endif %}

    {% set model_sql_size_limit = elementary.get_column_size() %}
    {% set model_code = elementary.get_compiled_code(node) %}

    {# Seeds do not have compiled code. #}
    {% if not model_code %}
        {{ return(none) }}
    {% endif %}

    {% if model_code | length > model_sql_size_limit %}
        {{ return('Model code is too long - over ' ~ model_sql_size_limit ~ ' bytes') }}
    {% else %}
        {{ return(model_code) }}
    {% endif %}
{% endmacro %}
