-- depends_on: {{ ref('current_schema_columns') }}
-- depends_on: {{ ref('previous_schema_columns') }}


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
        full_table_name,
        'type_changed' as change,
        column_name,
        cur.data_type as data_type,
        cur.is_nullable as is_nullable,
        column_name as pre_column_name,
        pre.data_type as pre_data_type,
        pre.is_nullable as pre_is_nullable,
        dbt_updated_at as detected_at
    from cur inner join pre using (full_table_name, column_name)
    where
        cur.data_type != pre.data_type
    ),

is_nullable_changes as (
     select
        full_table_name,
        'is_nullable' as change,
        column_name,
        cur.data_type as data_type,
        cur.is_nullable as is_nullable,
        column_name as pre_column_name,
        pre.data_type as pre_data_type,
        pre.is_nullable as pre_is_nullable,
        dbt_updated_at as detected_at
    from cur inner join pre using (full_table_name, column_name)
    where
        cur.is_nullable != pre.is_nullable
),

columns_added as (
        select
        full_table_name,
        'column_added' as change,
        column_name,
        cur.data_type as data_type,
        cur.is_nullable as is_nullable,
        null as pre_column_name,
        null as pre_data_type,
        null as pre_is_nullable,
        dbt_updated_at as detected_at
    from cur left join pre using (full_table_name, column_name)
    where pre.full_table_name is null and pre.column_name is null

),

columns_removed as (
        select
        full_table_name,
        'column_removed' as change,
        null as column_name,
        null as data_type,
        null as is_nullable,
        pre.column_name as pre_column_name,
        pre.data_type as pre_data_type,
        pre.is_nullable as pre_is_nullable,
        dbt_updated_at as detected_at
    from pre left join cur using (full_table_name, column_name)
    where cur.full_table_name is null and cur.column_name is null
),

all_column_changes_union as (
       select * from type_changes
        union all
        select * from is_nullable_changes
        union all
        select * from columns_added
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
        pre_column_name,
        pre_data_type,
        pre_is_nullable,
        detected_at
  from all_column_changes_union
)

select
  {{ dbt_utils.surrogate_key([
    'full_table_name',
    'column_name',
    'change'
    ]) }} as change_id,
    full_table_name,
    change,
    column_name,
    data_type,
    is_nullable,
    pre_column_name,
    pre_data_type,
    pre_is_nullable,
    detected_at
from all_column_changes
{% if is_incremental() %}
   where detected_at > (select max(detected_at) from {{ this }})
 {% endif %}
