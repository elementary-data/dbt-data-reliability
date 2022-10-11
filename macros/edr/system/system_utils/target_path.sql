{% macro get_target_path(filename) %}
  {{ return("%s/elementary/%s" % (ref.config.target_path, filename)) }}
{% endmacro %}
