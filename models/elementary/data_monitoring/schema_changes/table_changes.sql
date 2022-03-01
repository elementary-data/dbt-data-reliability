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

table_added as (
    select
        full_table_name,
        'table_added' as change,
        detected_at
    from cur
    where is_new = true

),

table_removed as (

    select
        pre.full_table_name,
        'table_removed' as change,
        pre.detected_at as detected_at
    from pre
    left join cur
        on (cur.full_table_name = pre.full_table_name and cur.full_schema_name = pre.full_schema_name)
    where cur.full_table_name is null
        and pre.full_schema_name in {{ elementary.strings_list_to_tuple(elementary.get_configured_schemas()) }}

),

all_table_changes as (

    select * from table_removed
    union all
    select * from table_added

),

table_changes_desc as (

    select
        {{ dbt_utils.surrogate_key(['full_table_name', 'change', 'detected_at']) }} as change_id,
        {{ elementary.full_name_to_db() }},
        {{ elementary.full_name_to_schema() }},
        {{ elementary.full_name_to_table() }},
        {{ elementary.run_start_column() }} as detected_at,
        change,

        case
            when change='table_added'
                then concat('The table "', full_table_name, '" was added')
            when change='table_removed'
                then concat('The table "', full_table_name, '" was removed')
            else NULL
        end as change_description

    from all_table_changes

)

select * from table_changes_desc
