-- Remove comparison to configuration

{{
  config(
    materialized = 'incremental',
    unique_key = 'change_id'
  )
}}

with cur as (

    select * from {{ ref('current_schema_columns')}}

),

pre as (

    select * from {{ ref('previous_schema_columns')}}

),

type_changes as (

    select
        cur.full_table_name,
        'type_changed' as change,
        cur.column_name,
        cur.data_type as data_type,
        pre.column_name as pre_column_name,
        pre.data_type as pre_data_type,
        cur.dbt_updated_at as detected_at
    from cur
    inner join pre
        on (cur.full_table_name = pre.full_table_name and cur.column_name = pre.column_name)
    where cur.data_type != pre.data_type

),

columns_added as (
    select
        cur.full_table_name,
        'column_added' as change,
        cur.column_name,
        cur.data_type as data_type,
        null as pre_column_name,
        null as pre_data_type,
        cur.dbt_updated_at as detected_at
    from cur
    left join pre
        on (cur.full_table_name = pre.full_table_name and cur.column_name = pre.column_name)
    where pre.full_table_name is null and pre.column_name is null

),

columns_removed as (

    select
        pre.full_table_name,
        'column_removed' as change,
        null as column_name,
        null as data_type,
        pre.column_name as pre_column_name,
        pre.data_type as pre_data_type,
        pre.dbt_updated_at as detected_at
    from pre
    left join cur
        on (cur.full_table_name = pre.full_table_name and cur.column_name = pre.column_name)
    where cur.full_table_name is null and cur.column_name is null

),


all_column_changes_union as (

    select * from type_changes
    union all
    select * from columns_removed
    union all
    select * from columns_added

),

all_column_changes as (

    select
        {{ dbt_utils.surrogate_key(['full_table_name', 'column_name', 'pre_column_name', 'change', 'detected_at']) }} as change_id,
        full_table_name,
        change,
        column_name,
        data_type,
        pre_column_name,
        pre_data_type,
        detected_at
    from all_column_changes_union

)

select * from all_column_changes
