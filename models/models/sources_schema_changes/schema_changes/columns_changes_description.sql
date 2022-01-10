{{
  config(
    materialized = 'incremental',
    unique_key = 'change_id'
  )
}}

with columns_changes as (

    select * from {{ref ('columns_changes')}}

),

columns_changes_desc as (

    select
        change_id,
        full_table_name,
        case
            when change = 'column_removed' then pre_column_name
            else column_name
        end as column_name,
        detected_at,
        change,
        case
            when change= 'column_added' then concat('the column "',column_name,'" was added')
            when change= 'column_removed' then concat('the column "',pre_column_name,'" was removed')
            when change= 'type_changed_from_configuration' then concat('the type of "',column_name,'" was changed to ',data_type,' and is configured to be: ',pre_data_type)
            when change= 'type_changed' then concat('the type of "',column_name,'" was changed to ',data_type,' from: ',pre_data_type)
            else 'no description'
        end
        as change_description,
        case
            when change= 'column_added' then concat('data type: ', data_type)
            when change= 'column_removed' then concat('data type: ', pre_data_type)
            else null
        end
        as change_info
    from columns_changes

)

select * from columns_changes_desc
