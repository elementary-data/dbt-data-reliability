{{
  config(
    materialized='ephemeral'
  )
}}

with current_and_previous_schemas as (

    select * from {{ ref('current_and_previous_columns') }}

),

flat_previous_jsons as (

    select
        full_table_name,
        f.value as columns_jsons
    from current_and_previous_schemas,
    table (flatten(current_and_previous_schemas.previous_schema)) f
    where previous_schema is not null

),

previous_schemas_columns as (

    select
        full_table_name,
        {{trim_quotes('columns_jsons:column_name')}} as column_name,
        {{trim_quotes('columns_jsons:data_type')}} as data_type
    from flat_previous_jsons

)

select * from previous_schemas_columns
