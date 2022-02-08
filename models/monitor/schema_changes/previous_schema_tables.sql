{{
  config(
    materialized='ephemeral'
  )
}}

with schemas_snapshot as (

    select * from {{ ref('schema_tables_snapshot') }}
),

schemas_order as (

    select *,
        row_number() over (partition by full_schema_name order by dbt_updated_at desc) as schema_order
    from schemas_snapshot

),

previous_schemas as (

    select *
    from schemas_order
    where schema_order = 2

),

flat_previous_tables as (

    select
        full_schema_name,
        dbt_updated_at,
        concat(full_schema_name, '.', {{ trim_quotes('f.value') }}) as full_table_name
    from previous_schemas,
    table (flatten(previous_schemas.tables_in_schema)) f

)

select * from flat_previous_tables
