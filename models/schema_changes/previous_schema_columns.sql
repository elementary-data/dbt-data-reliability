{{
  config(
    materialized='ephemeral'
  )
}}

with schemas_snapshot as (

    select * from {{ ref('table_columns_snapshot') }}
),

schemas_order as (

    select *,
        row_number() over (partition by full_table_name order by dbt_updated_at desc) as schema_order
    from schemas_snapshot

),

previous_schemas as (

    select *
    from schemas_order
    where schema_order = 2

),

flat_previous_columns as (

    select
        full_table_name,
        dbt_updated_at,
        f.value as columns_jsons
    from previous_schemas,
    table (flatten(previous_schemas.columns_schema)) f

),

previous_schemas_columns as (

    select
        full_table_name,
        dbt_updated_at,
        {{trim_quotes('columns_jsons:column_name')}} as column_name,
        {{trim_quotes('columns_jsons:data_type')}} as data_type
    from flat_previous_columns

)

select * from previous_schemas_columns
