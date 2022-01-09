{{
    config(
        materialized = 'incremental',
        unique_key = 'change_id'
    )
}}

with columns_changes as (
    select * from {{ref ('columns_changes')}}
),

tables_changes as (
    select * from {{ref ('tables_changes')}}
),

columns_changes_desc as (
  select
  change_id,
  full_table_name,
  detected_at,
  change,
  case when change='column_added' then concat('the column "',column_name,'" was added')
  when change='column_removed' then concat('the column "',pre_column_name,'" was removed')
  when change='type_changed' then concat('the type of "',column_name,'" was changed to ',data_type,' from: ',pre_data_type)
  when change='is_nullable' then concat('the "is_nullable" of "',column_name,'" was changed to ',is_nullable,' from: ',pre_is_nullable)
  else 'no description'
  end
  as description,
  case when change='column_added'then concat('data type: ', data_type, ', is nullable: ',is_nullable)
  when change='column_removed' then concat('data type: ', pre_data_type, ', is nullable: ', pre_is_nullable)
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
  when change='table_added' then concat('the table "',full_table_name, '" was added')
  when change='table_removed' then concat('the table "',full_table_name, '" was removed')
  else 'no description'
  end
  as description,
  case
  when change='table_added' then to_varchar(current_schema)
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
