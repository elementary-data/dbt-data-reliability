{{
  config(
    materialized = 'view',
  )
}}

{% set configured_schemas = elementary.get_configured_schemas_from_graph() %}

{{ print("============================================") }}
{{ print("configured_schemas: " ~ toyaml(configured_schemas)) }}
{{ print("============================================") }}

{{ print("============================================") }}
{{ print("columns" ~ elementary.get_columns_by_schemas(configured_schemas)) }}
{{ print("============================================") }}

with filtered_information_schema_columns as (
    {{ elementary.get_columns_by_schemas(configured_schemas) }}
)

select *
from filtered_information_schema_columns
where full_table_name is not null
