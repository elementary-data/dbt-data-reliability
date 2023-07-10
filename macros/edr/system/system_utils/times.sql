{% macro get_time_format() %}
  {% do return("%Y-%m-%d %H:%M:%S") %}
{% endmacro %}

{% macro run_started_at_as_string() %}
    {% do return(elementary.get_run_started_at().strftime(elementary.get_time_format())) %}
{% endmacro %}

{% macro datetime_now_utc_as_string() %}
    {% set now = modules.datetime.datetime.utcnow() %}
    {% if elementary.get_config_var('custom_run_started_at') %}
      {% set custom_run_started_at = elementary.get_run_started_at() %}
      {% set timediff = custom_run_started_at - run_started_at.replace(tzinfo=None) %}
      {% set now = now + timediff %}
    {% endif %}
    {% do return(now.strftime(elementary.get_time_format())) %}
{% endmacro %}

{% macro current_timestamp_column() %}
    cast ({{elementary.edr_current_timestamp_in_utc()}} as {{ elementary.edr_type_timestamp() }})
{% endmacro %}

{% macro datetime_now_utc_as_timestamp_column() %}
    cast ('{{ elementary.datetime_now_utc_as_string() }}' as {{ elementary.edr_type_timestamp() }})
{% endmacro %}
