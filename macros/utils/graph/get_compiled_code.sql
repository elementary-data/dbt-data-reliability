{% macro get_compiled_code(node) %}
    {% do return(adapter.dispatch("get_compiled_code", "elementary")(node)) %}
{% endmacro %}

{% macro default__get_compiled_code(node) %}
    {% do return(node.get('compiled_code') or node.get('compiled_sql')) %}
{% endmacro %}

{% macro redshift__get_compiled_code(node) %}
    {% set compilde_code = node.get('compiled_code') or node.get('compiled_sql') %}
    {% if not compilde_code %}
        {% do return(none) %}
    {% else %}
        {% do return(compilde_code.replace("%", "%%")) %}
    {% endif %}
{% endmacro %}
