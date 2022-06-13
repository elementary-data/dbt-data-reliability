{% macro get_test_argument(argument_name, value=none) %}
  {% if value %}
    {{ return(value) }}
  {% else %}
    {{ return(elementary.get_config_var(argument_name)) }}
  {% endif %}
{% endmacro %}
