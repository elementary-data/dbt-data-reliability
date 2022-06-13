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
        pre.data_type as pre_data_type,
        pre.detected_at
    from cur inner join pre
        on (cur.full_table_name = pre.full_table_name and cur.column_name = pre.column_name)
    where cur.data_type != pre.data_type

),

columns_added as (

    select
        full_table_name,
        'column_added' as change,
        column_name,
        data_type,
        {{ elementary.null_string() }} as pre_data_type,
        detected_at as detected_at
    from cur
    where is_new = true

),

columns_removed as (

    select
        pre.full_table_name,
        'column_removed' as change,
        pre.column_name as column_name,
        {{ elementary.null_string() }} as data_type,
        pre.data_type as pre_data_type,
        pre.detected_at as detected_at
    from pre left join cur
        on (cur.full_table_name = pre.full_table_name and cur.column_name = pre.column_name)
    where cur.full_table_name is null and cur.column_name is null

),

columns_removed_filter_deleted_tables as (

    select
        removed.full_table_name,
        removed.change,
        removed.column_name,
        removed.data_type,
        removed.pre_data_type,
        removed.detected_at
    from columns_removed as removed join cur
        on (removed.full_table_name = cur.full_table_name)

),

all_column_changes as (

    select * from type_changes
    union all
    select * from columns_removed_filter_deleted_tables
    union all
    select * from columns_added

),

column_changes_desc as (

    select
        {{ dbt_utils.surrogate_key(['full_table_name', 'column_name', 'change', 'detected_at']) }} as change_id,
        {{ elementary.full_name_split('database_name') }},
        {{ elementary.full_name_split('schema_name') }},
        {{ elementary.full_name_split('table_name') }},
        column_name,
        {{ elementary.run_start_column() }} as detected_at,
        change,
        case
            when change= 'column_added'
                then 'The column "' || column_name || '" was added'
            when change= 'column_removed'
                then 'The column "' || column_name || '" was removed'
            when change= 'type_changed'
                then 'The type of "' || column_name || '" was changed from ' || pre_data_type || ' to ' || data_type
            else NULL
        end as change_description
    from all_column_changes

)

select
    {{ elementary.cast_as_string('change_id') }} as change_id,
    {{ elementary.cast_as_string('database_name') }} as database_name,
    {{ elementary.cast_as_string('schema_name') }} as schema_name,
    {{ elementary.cast_as_string('table_name') }} as table_name,
    {{ elementary.cast_as_string('column_name') }} as column_name,
    {{ elementary.cast_as_timestamp('detected_at') }} as detected_at,
    {{ elementary.cast_as_string('change') }} as change,
    {{ elementary.cast_as_long_string('change_description') }} as change_description
from column_changes_desc