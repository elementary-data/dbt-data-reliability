{% macro lag(column, offset=1) %}
    {{ return(adapter.dispatch('lag', 'elementary')(column, offset)) }}
{% endmacro %}

{% macro default__lag(column, offset=1) %}
    lag({{ column }}, {{ offset }})
{% endmacro %}

{% macro clickhouse__lag(column, offset=1) %}
    lagInFrame({{ column }}, {{ offset }})
{% endmacro %}
