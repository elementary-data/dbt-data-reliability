{% macro upload_information() %}
  {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
  {% if not execute or edr_cli_run %}
    {{ return('') }}
  {% endif %}

  {% set identifier = 'information' %}
  {% set relation = elementary.get_elementary_relation(identifier) %}
  {% if not relation %}
    {{ return('') }}
  {% endif %}

  {% set data = [
    {'key': 'dbt_version', 'value': dbt_version},
    {'key': 'elementary_version', 'value': elementary.get_elementary_package_version()},
  ] %}
  {% do dbt.truncate_relation(relation) %}
  {% do elementary.insert_rows(relation, data) %}
{% endmacro %}
