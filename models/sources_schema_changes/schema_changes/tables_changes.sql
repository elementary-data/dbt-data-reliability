{{
  config(
    materialized = 'incremental',
    unique_key = 'change_id'
  )
}}

with cur as (

    select * from {{ ref('current_schema_tables')}}

),

pre as (

    select * from {{ ref('previous_schema_tables')}}

),

columns_schemas as (

    select * from {{ ref('current_and_previous_columns')}}

),

tables_added as (
    select
        cur.full_table_name,
        'table_added' as change,
        cur.dbt_updated_at as detected_at
    from cur
    left join pre
        on (cur.full_table_name = pre.full_table_name and cur.full_schema_name = pre.full_schema_name)
    where pre.full_table_name is null

),

tables_removed as (

    select
        pre.full_table_name,
        'table_removed' as change,
        pre.dbt_updated_at as detected_at
    from pre
    left join cur
        on (cur.full_table_name = pre.full_table_name and cur.full_schema_name = pre.full_schema_name)
    where cur.full_table_name is null

),


union_tables_changes as (

    select * from tables_removed
    union all
    select * from tables_added

),

tables_changes as (

    select
        {{ dbt_utils.surrogate_key(['tables.full_table_name', 'tables.change', 'tables.detected_at']) }} as change_id,
        upper(substr(tables.full_table_name, 1, regexp_instr(full_table_name, '\\.' ,1, 2)-1)) as full_schema_name,
        tables.full_table_name,
        tables.change,
        columns.current_schema as table_schema,
        tables.detected_at
    from union_tables_changes as tables
    left join columns_schemas as columns
        on (tables.full_table_name = columns.full_table_name)

)

select * from tables_changes
