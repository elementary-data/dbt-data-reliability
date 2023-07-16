{% macro generate_elementary_cli_profile(method=none) %}
  {% set profile_parameters = elementary.generate_elementary_profile_parameters(method) %}
  {% if profile_parameters is string %}
    {% set profile = profile_parameters %}
  {% else %}
    {% set profile = elementary.cli_profile_from_parameters(profile_parameters)%}
  {% endif %}
  {{ log('\n' ~ profile, info=True) }}
{% endmacro %}

{% macro cli_profile_from_parameters(parameters) %}
elementary:
  outputs:
    default:
      {% for parameter in parameters -%}
      {%- set key = parameter[0] -%}
      {%- set value = parameter[1] -%}
      {%- if value is string -%}
        {%- set value = '"' ~ value ~ '"' -%}
      {%- endif -%}
      {%- if parameters|length > 2 -%}
        {%- set comment = parameter[2] -%}
      {%- endif -%}
      {{ key }}: {{ value }}{% if comment %}  # {{ comment }}{% endif %}
      {% endfor -%}
{% endmacro %}
