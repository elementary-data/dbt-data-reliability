{% macro get_incremental_microbatch_sql(arg_dict) %}
  {{ return(elementary.get_incremental_microbatch_sql(arg_dict)) }}
{% endmacro %}
