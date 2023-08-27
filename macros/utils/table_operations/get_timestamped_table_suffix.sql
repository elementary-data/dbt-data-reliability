{% macro get_timestamped_table_suffix() %}
    {% do return(modules.datetime.datetime.utcnow().strftime('__tmp_%Y%m%d%H%M%S%f')) %}
{% endmacro %}
