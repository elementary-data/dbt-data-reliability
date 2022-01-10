{{
  config(
    materialized='ephemeral'
  )
}}

with current_and_previous_schemas as (

    select * from {{ ref('current_and_previous_schemas') }}

),

flat_jsons_cs as (

    select
        full_table_name,
        dbt_updated_at,
        f.value as columns_jsons
    from current_and_previous_schemas,
    table(flatten(current_and_previous_schemas.current_schema)) f
    where previous_schema is not null

),

current_schemas_col as (

    select
        full_table_name,
        dbt_updated_at,
        {{trim_quotes('columns_jsons:column_name')}} as column_name,
        {{trim_quotes('columns_jsons:data_type')}} as data_type
    from flat_jsons_cs

)

select * from current_schemas_col
