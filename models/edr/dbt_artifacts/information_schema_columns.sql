{{
  config(
    materialized=elementary.get_information_schema_columns_materialized(),
  )
}}
{% if elementary.get_information_schema_columns_materialized() == "view" %}
    {{ elementary.get_information_schema_columns_query() }}
{% else %}
    {{ elementary.get_empty_columns_from_information_schema_table() }}
{% endif %}
