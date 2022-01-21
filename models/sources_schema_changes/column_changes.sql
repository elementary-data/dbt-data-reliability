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
        pre.column_name as column_name,
        null as data_type,
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

column_changes_desc as (

    select
        {{ dbt_utils.surrogate_key(['full_table_name', 'column_name', 'change', 'detected_at']) }} as change_id,
        full_table_name,
        column_name,
        detected_at,
        change,

        case
            when change= 'column_added'
                then concat('The column "', column_name,'" was added')
            when change= 'column_removed'
                then concat('The column "', column_name,'" was removed')
            when change= 'type_changed'
                then concat('The type of "',column_name,'" was changed from ', pre_data_type,' to: ', data_type)
            else NULL
        end as change_description

    from all_column_changes_union

),

column_changes_with_full_name as (

    select
        *,
        concat(full_table_name, '.', column_name) as full_column_name
    from column_changes_desc

)

select * from column_changes_with_full_name