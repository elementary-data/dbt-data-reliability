{% macro run_started_at_as_string(time_format=elementary.get_config_var('time_format')) %}
    {{ return(run_started_at.strftime(time_format)) }}
{% endmacro %}

{% macro datetime_now_utc_as_string(time_format=elementary.get_config_var('time_format')) %}
    {% set current_timestamp_as_string = modules.datetime.datetime.utcnow().strftime(time_format) %}
    {{ return(current_timestamp_as_string) }}
{% endmacro %}
