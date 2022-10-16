{% macro get_target_path(filename) %}
  {{ return(flags.Path(ref.config.target_path) / 'elementary' / filename) }}
{% endmacro %}
