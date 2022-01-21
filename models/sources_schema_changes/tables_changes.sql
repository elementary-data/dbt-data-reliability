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

tables_changes_desc as (

    select
        {{ dbt_utils.surrogate_key(['full_table_name', 'change', 'detected_at']) }} as change_id,
        {{ full_table_name_to_schema() }},
        full_table_name,
        detected_at,
        change,

        case
            when change='table_added'
                then concat('The table "', full_table_name, '" was added')
            when change='table_removed'
                then concat('The table "', full_table_name, '" was removed')
            else 'no description'
        end as change_description

    from union_tables_changes

)

select * from tables_changes_desc
