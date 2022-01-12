{{
  config(
    materialized='ephemeral'
  )
}}

with current_and_previous_tables as (

    select * from {{ ref('current_and_previous_tables') }}

),

flat_tables_array as (

    select
        full_schema_name,
        dbt_updated_at,
        {{ trim_quotes('f.value') }} as full_table_name
    from current_and_previous_tables,
    table (flatten(current_and_previous_tables.current_tables)) f
    where previous_tables is not null

)

select * from flat_tables_array
