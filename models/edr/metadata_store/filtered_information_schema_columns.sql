{{
  config(
    materialized = 'view',
  )
}}

{% set configured_schemas = elementary.get_configured_schemas_from_graph() %}

{{ log("============================================") }}
{{ log("configured_schemas: " ~ toyaml(configured_schemas)) }}
{{ log("============================================") }}

with filtered_information_schema_columns as (
    {{ elementary.get_columns_by_schemas(configured_schemas) }}
)

select *
from filtered_information_schema_columns
where full_table_name is not null
