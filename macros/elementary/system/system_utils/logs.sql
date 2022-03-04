{% macro edr_log(msg) %}
    {%- if execute %}
        {% do log('Elementary: ' ~ msg, info=True) %}
    {%- endif %}
{% endmacro %}

{% macro debug_log(msg) %}
    {%- if execute %}
        {% do log('EDR_LOG_PRE', info=True) %}
        {% do log(msg, info=True) %}
        {% do log('EDR_LOG_AFTER', info=True) %}
    {%- endif %}
{% endmacro %}