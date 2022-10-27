{% macro surrogate_key() %}
  {% set macro = dbt_utils.generate_surrogate_key or dbt_utils.surrogate_key %}
  {% if not macro %}
    {{ exceptions.raise_compiler_error("Did not find a surrogate_key macro.") }}
  {% endif %}
  {{ return(macro) }}
{% endmacro %}
