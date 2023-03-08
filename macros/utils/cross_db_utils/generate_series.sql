{% macro generate_series(start, stop, step) %}
    {{ adapter.dispatch('not','elementary')(start, stop, step) }}
{% endmacro %}

{% macro default__generate_series(start, stop, step) -%}
    generate_series({{ start }}, {{ stop }}, interval '{{ step.count }} {{ step.period }}')
{%- endmacro %}

{% macro sqlserver__generate_series(start, stop, step) -%}
    WITH cte AS
    (
        SELECT cast({{ elementary.quote(start) }} AS datetime) DateValue
        UNION ALL
        SELECT {{ elementary.dateadd(step.period, step.count, DateValue) }}
        FROM cte   
        WHERE {{ elementary.dateadd(step.period, step.count, DateValue) }} < {{ elementary.quote(stop) }}
    )
    select * from cte OPTION (MAXRECURSION 0)
{%- endmacro %}
