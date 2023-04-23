{{
  config(
    materialized = 'view',
    enabled = target.type != 'databricks' and target.type != 'spark' | as_bool()
  )
}}

with dbt_models_data as (
    select * from {{ ref('dbt_models') }}
),

columns_information as (
    {{ elementary.get_columns_in_project() }}
),

dbt_columns as (
    select col_info.*
    from dbt_models_data models
    join columns_information col_info
        on (lower(models.database_name) = lower(col_info.database_name) and
            lower(models.schema_name) = lower(col_info.schema_name) and
            lower(models.name) = lower(col_info.table_name)
        )
)

select *
from dbt_columns
