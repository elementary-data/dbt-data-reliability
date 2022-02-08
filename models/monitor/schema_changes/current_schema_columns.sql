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

current_and_previous_columns as (

    select
        cur.full_table_name,
        cur.columns_schema as current_schema,
        pre.columns_schema as previous_schema,
        cur.dbt_updated_at
    from current_schemas cur
    left join previous_schemas pre
        on (cur.full_table_name = pre.full_table_name)

),

flat_current_columns as (

    select
        full_table_name,
        dbt_updated_at,
        f.value as columns_jsons
    from current_and_previous_columns,
    table(flatten(current_and_previous_columns.current_schema)) f
    where previous_schema is not null

),

current_schemas_columns as (

    select
        full_table_name,
        dbt_updated_at,
        {{trim_quotes('columns_jsons:column_name')}} as column_name,
        {{trim_quotes('columns_jsons:data_type')}} as data_type
    from flat_current_columns

)

select * from current_schemas_columns
