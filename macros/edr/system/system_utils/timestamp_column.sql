{% macro run_start_column() %}
    cast ('{{ elementary.get_run_started_at().strftime("%Y-%m-%d %H:%M:%S") }}' as {{ elementary.type_timestamp() }})
{% endmacro %}

{% macro current_timestamp_column() %}
    cast ({{elementary.current_timestamp_in_utc()}} as {{ elementary.type_timestamp() }})
{% endmacro %}

{% macro formatted_current_timestamp_column() %}
    cast ('{{ modules.datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") }}' as {{ elementary.type_timestamp() }})
{% endmacro %}
