{% macro get_target_path(identifier=none) %}
  {% set elementary_target_path = flags.Path(ref.config.target_path) / 'elementary' %}
  {% if identifier %}
    {{ return(elementary_target_path / (identifier | string)) }}
  {% else %}
    {{ return(elementary_target_path) }}
  {% endif %}
{% endmacro %}
