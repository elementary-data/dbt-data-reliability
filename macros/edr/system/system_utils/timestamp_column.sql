{% macro run_start_column() %}
    cast ('{{ elementary.get_run_started_at().strftime("%Y-%m-%d %H:%M:%S") }}' as {{ elementary.elementary_type_timestamp() }})
{% endmacro %}

{% macro current_timestamp_column() %}
    cast ({{elementary.elementary_current_timestamp_in_utc()}} as {{ elementary.elementary_type_timestamp() }})
{% endmacro %}

{% macro datetime_now_utc_as_timestamp_column() %}
    cast ('{{ elementary.datetime_now_utc_as_string() }}' as {{ elementary.elementary_type_timestamp() }})
{% endmacro %}
