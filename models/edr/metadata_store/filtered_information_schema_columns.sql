{{
  config(
    materialized = 'view',
    enabled = target.type != 'databricks' and target.type != 'spark' | as_bool()
  )
}}

{% set configured_databases = elementary.get_configured_databases_from_graph() %}

with filtered_information_schema_columns as (
    {{ elementary.get_columns_by_configured_databases(configured_databases) }}
)

select *
from filtered_information_schema_columns
where full_table_name is not null