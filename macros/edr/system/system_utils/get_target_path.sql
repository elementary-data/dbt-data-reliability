{% macro get_target_path(identifier=none) %}
  {% set runtime_conf = elementary.get_runtime_config() %}
  {% set elementary_target_path = flags.Path(runtime_conf.target_path) / 'elementary' %}
  {% if identifier %}
    {{ return(elementary_target_path / identifier) }}
  {% else %}
    {{ return(elementary_target_path) }}
  {% endif %}
{% endmacro %}
