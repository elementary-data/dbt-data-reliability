{% macro generate_series(start, stop, step, column_name) %}
    {{ adapter.dispatch('generate_series','elementary')(start, stop, step, column_name) }}
{% endmacro %}

{% macro default__generate_series(start, stop, step, column_name) -%}
    generate_series({{ start }}, {{ stop }}, interval '{{ step.count }} {{ step.period }}') {{ column_name }}
{%- endmacro %}

{% macro sqlserver__generate_series(start, stop, step, column_name) -%}

    {%- set max_period_results = run_query(' select ' ~ elementary.datediff(start, stop, step.period)) -%}
    {%- if execute -%}
        {%- set max_period = max_period_results.columns[0].values()[0] -%}
    {%- else -%}
        {%- set max_period = 0 -%}
    {%- endif -%}
    {%- set max_period = max_period|int -%}
    (
    {% for interval in range(0,max_period) -%}
        select {{ elementary.dateadd(step.period, interval, start) }} {{ column_name }}
        {%- if not loop.last %} union {%- endif %}
    {% endfor %}
    ) DateSeries
{%- endmacro %}
