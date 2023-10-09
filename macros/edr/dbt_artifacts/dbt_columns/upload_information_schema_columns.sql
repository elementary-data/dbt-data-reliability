{% macro upload_information_schema_columns() %}
  {% set relation = elementary.get_elementary_relation("information_schema_columns") %}
  {% if execute and relation %}
    {% set information_schema_columns_query = elementary.get_information_schema_columns_query(is_model_build_context=false) %}
    {% do elementary.create_or_replace(false, relation, information_schema_columns_query) %}
  {% endif %}
{% endmacro %}
