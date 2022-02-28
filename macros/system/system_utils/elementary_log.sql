{% macro elementary_log(msg) %}
    {% do log('Elementary: ', msg, info=True) %}
{% endmacro %}