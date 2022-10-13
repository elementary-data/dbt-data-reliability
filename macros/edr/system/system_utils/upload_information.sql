{% macro upload_information() %}
  {% set relation = elementary.get_elementary_relation('information') %}
  {% if not relation %}
    {{ return('') }}
  {% endif %}

  {% set data = [
    {'key': 'dbt_version', 'value': dbt_version},
    {'key': 'elementary_version', 'value': elementary.get_elementary_package_version()},
  ] %}

  {% do adapter.truncate_relation(relation) %}
  {% do elementary.insert_dicts(relation, data) %}
{% endmacro %}
