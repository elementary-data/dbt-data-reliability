{% macro edr_log(msg) %}
    {% do log('Elementary: ' ~ msg, info=True) %}
{% endmacro %}

{% macro debug_log(msg) %}
    {% do log('EDR_LOG_PRE', info=True) %}
    {% do log(msg, info=True) %}
    {% do log('EDR_LOG_AFTER', info=True) %}
{% endmacro %}