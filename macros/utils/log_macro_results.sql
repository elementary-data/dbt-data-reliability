{% macro log_macro_results(macro_name, macro_args=none) %}
    {% if macro_args is none %}
        {% set macro_args = {} %}
    {% endif %}
    {% set results = context[macro_name](**macro_args) %}
    {% do elementary.edr_log(tojson(results)) %}
{% endmacro %}
