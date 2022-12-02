{% macro create_target_dir() %}
  {% do elementary.get_target_path().mkdir(parents=true, exist_ok=true) %}
{% endmacro %}
