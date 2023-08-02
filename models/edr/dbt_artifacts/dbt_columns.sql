{{
  config(
    materialized=elementary.get_dbt_columns_materialization(),
  )
}}
{% if elementary.get_dbt_columns_materialization() == "view" %}
    {{ elementary.get_dbt_columns_query() }}
{% else %}
    {{ elementary.get_empty_columns_from_information_schema_table() }}
{% endif %}
