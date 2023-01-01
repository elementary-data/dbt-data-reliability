{% macro get_utc_tzinfo() %}
  {% do return(run_started_at.tzinfo) %}
{% endmacro %}

{% macro get_time_format() %}
  {% do return("%Y-%m-%d %H:%M:%S") %}
{% endmacro %}

{% macro run_started_at_as_string() %}
    {% do return(elementary.get_run_started_at().strftime(elementary.get_time_format())) %}
{% endmacro %}

{% macro datetime_now_utc_as_string() %}
    {% set utcnow = modules.datetime.datetime.utcnow().replace(tzinfo=elementary.get_utc_tzinfo()) %}
    {% set custom_run_started_at = elementary.get_run_started_at() %}
    {% set custom_now = custom_run_started_at + (utcnow - run_started_at) %}
    {% do return(custom_now.strftime(elementary.get_time_format())) %}
{% endmacro %}
