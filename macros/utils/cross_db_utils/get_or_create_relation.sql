{% macro get_or_create_relation(database, schema, identifier, type) %}
  {% set database, schema, identifier = elementary.make_match_kwargs(database, schema, identifier) %}
  {% set relation_existed, target_relation = dbt.get_or_create_relation(database, schema, identifier, type) %}
  {% if not relation_existed %}
    {{ adapter.cache_added(target_relation) }}
  {% endif %}
  {{ return([relation_existed, target_relation]) }}
{% endmacro %}
