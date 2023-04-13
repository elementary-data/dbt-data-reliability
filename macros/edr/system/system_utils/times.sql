{% macro get_time_format() %}
  {% do return("%Y-%m-%d %H:%M:%S") %}
{% endmacro %}

{% macro run_started_at_as_string() %}
    {% do return(elementary.get_run_started_at().strftime(elementary.get_time_format())) %}
{% endmacro %}

{% macro datetime_now_utc_as_string() %}
    {% do return(modules.datetime.datetime.utcnow().strftime(elementary.get_time_format())) %}
{% endmacro %}

{% macro current_timestamp_column() %}
    cast ({{elementary.edr_current_timestamp_in_utc()}} as {{ elementary.edr_type_timestamp() }})
{% endmacro %}

{% macro datetime_now_utc_as_timestamp_column() %}
    cast ('{{ elementary.datetime_now_utc_as_string() }}' as {{ elementary.edr_type_timestamp() }})
{% endmacro %}
