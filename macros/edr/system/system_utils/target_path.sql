{% macro get_target_path(filename) %}
  {{ return(flags.Path("%s/elementary/%s" % (ref.config.target_path, filename))) }}
{% endmacro %}
