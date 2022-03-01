{% macro row_count_monitor() -%}
    count(1)
{%- endmacro %}

{% macro freshness_monitor(timestamp_field, timeframe_end) -%}
    {%- if not timestamp_field %}
         cast (null as {{ dbt_utils.type_int()}})
    {%- else %}
         {{ elementary.freshness_check(timestamp_field, timeframe_end) }}
    {%- endif %}
{%- endmacro %}

{% macro freshness_check( timestamp_field, timeframe_end) %}
    {{ adapter.dispatch('freshness_check')( timestamp_field, timeframe_end) }}
{% endmacro %}

{% macro default__freshness_check( timestamp_field, timeframe_end) %}
    timediff(minute, max({{ timestamp_field }}), {{ timeframe_end}})
{% endmacro %}

{% macro bigquery__freshness_check( timestamp_field, timeframe_end) %}
    timestamp_diff( timestamp({{ timeframe_end }}), timestamp(max({{ timestamp_field }})), minute)
{% endmacro %}
