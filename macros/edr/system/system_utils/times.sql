{% macro current_timestamp_utc_as_string(time_format=elementary.get_config_var('time_format')) %}
    modules.datetime.datetime.utcnow().strftime(time_format)
{% endmacro %}
