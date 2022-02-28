{% macro edr_log(msg) %}
    {% do log('EDR_LOG_PRE', info=True) %}
    {% do log(msg, info=True) %}
    {% do log('EDR_LOG_AFTER', info=True) %}
{% endmacro %}