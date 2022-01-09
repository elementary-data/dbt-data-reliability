-- depends_on: {{ ref('current_schema_columns') }}

{{
    config(
        materialized = 'incremental',
        unique_key = 'change_id'
    )
}}

with cur as (
select * from {{ ref('current_schema_columns')}}
),

reg as (
select * from {{ ref('sources_schema_registry')}}
),

type_changes as (
    select
        full_table_name,
        'type_changed' as change,
        column_name,
        cur.data_type as data_type,
        cur.is_nullable as is_nullable,
        column_name as reg_column_name,
        reg.data_type as reg_data_type,
        reg.is_nullable as reg_is_nullable,
        dbt_updated_at as detected_at
    from cur inner join reg using (full_table_name, column_name)
    where
        cur.data_type != reg.data_type
    ),
-- placeholder for adding is_nullable to config
is_nullable_changes as (
     select
        full_table_name,
        'is_nullable' as change,
        column_name,
        cur.data_type as data_type,
        cur.is_nullable as is_nullable,
        column_name as reg_column_name,
        reg.data_type as reg_data_type,
        reg.is_nullable as reg_is_nullable,
        dbt_updated_at as detected_at
    from cur inner join reg using (full_table_name, column_name)
    where
        cur.is_nullable != reg.is_nullable
    -- remove this after adding is_nullable to config
        and reg.is_nullable is not null
),

columns_removed as (
        select
        full_table_name,
        'column_removed' as change,
        null as column_name,
        null as data_type,
        null as is_nullable,
        reg.column_name as reg_column_name,
        reg.data_type as reg_data_type,
        reg.is_nullable as reg_is_nullable,
        dbt_updated_at as detected_at
    from reg left join cur using (full_table_name, column_name)
    where cur.full_table_name is null and cur.column_name is null
),

all_column_changes_union as (
       select * from type_changes
        union all
        select * from is_nullable_changes
        union all
        select * from columns_removed
),

all_column_changes as (
 select
        full_table_name,
        change,
        column_name,
        data_type,
        is_nullable,
        reg_column_name,
        reg_data_type,
        reg_is_nullable,
        detected_at
  from all_column_changes_union
)

select
  {{ dbt_utils.surrogate_key([
    'full_table_name',
    'column_name',
    'change',
    ]) }} as change_id,
    full_table_name,
    change,
    column_name,
    data_type,
    is_nullable,
    reg_column_name,
    reg_data_type,
    reg_is_nullable,
    detected_at
from all_column_changes
{% if is_incremental() %}
  where detected_at > (select max(detected_at) from {{ this }})
{% endif %}
