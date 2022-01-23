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

current_schemas as (

    select *
    from schemas_order
    where schema_order = 1

),

previous_schemas as (

    select *
    from schemas_order
    where schema_order = 2

),

current_and_previous_tables as (

    select
        cur.full_schema_name,
        cur.tables_in_schema as current_tables,
        pre.tables_in_schema as previous_tables,
        cur.dbt_updated_at
    from current_schemas cur
    left join previous_schemas pre
        on (cur.full_schema_name = pre.full_schema_name)

),

flat_current_tables as (

    select
        full_schema_name,
        dbt_updated_at,
        concat(full_schema_name, '.', {{ trim_quotes('f.value') }}) as full_table_name
    from current_and_previous_tables,
    table (flatten(current_and_previous_tables.current_tables)) f
    where previous_tables is not null

)

select * from flat_current_tables
