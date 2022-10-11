{% macro get_agate_table() %}
  {% do return(flags.os.sys.modules['agate'].Table) %}
{% endmacro %}
