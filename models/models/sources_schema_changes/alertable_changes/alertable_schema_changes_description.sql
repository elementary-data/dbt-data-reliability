{{
    config(
        materialized = 'incremental',
        unique_key = 'change_id'
    )
}}

with columns_changes as (
    select * from {{ref ('columns_changes_from_configuration')}}
),

tables_changes as (
    select * from {{ref ('removed_monitored_tables')}}
),

columns_changes_desc as (
  select
  change_id,
  full_table_name,
  detected_at,
  change,
  case
  when change='column_removed' then concat('the column "',reg_column_name,'" was removed')
  when change='type_changed' then concat('the type of "',column_name,'" was changed to ',data_type,' from: ',reg_data_type)
  when change='is_nullable' then concat('the "is_nullable" of "',column_name,'" was changed to ',is_nullable,' from: ',reg_is_nullable)
  else 'no description'
  end
  as description,
  case
  when change='column_removed' then concat('data type: ', reg_data_type, ', is nullable: ', reg_is_nullable)
  else null
  end
  as info
  from columns_changes
),

tables_changes_desc as (
  select
  change_id,
  full_table_name,
  detected_at,
  change,
  case
  when change='table_removed' then concat('the table "',full_table_name, '" was removed')
  else 'no description'
  end
  as description,
  case
  when change='table_removed' then to_varchar(current_schema)
  else null
  end
  as info
  from tables_changes
),

final as (
    select * from tables_changes_desc
      union all
    select * from columns_changes_desc
)

select * from final
{% if is_incremental() %}
  where detected_at > (select max(detected_at) from {{ this }})
{% endif %}
