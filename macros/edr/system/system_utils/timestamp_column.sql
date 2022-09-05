{% macro run_start_column() %}
    cast ('{{ elementary.get_run_started_at().strftime("%Y-%m-%dT%H:%M:%S.%fZ") }}' as {{ elementary.type_timestamp() }})
{% endmacro %}

{% macro current_timestamp_column() %}
    cast ({{elementary.current_timestamp_in_utc()}} as {{ elementary.type_timestamp() }})
{% endmacro %}

{% macro current_timestamp_utc_now_column() %}
    cast ('{{ modules.datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ") }}' as {{ elementary.type_timestamp() }})
{% endmacro %}