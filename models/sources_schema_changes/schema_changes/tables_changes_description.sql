{{
  config(
    materialized = 'incremental',
    unique_key = 'change_id'
  )
}}

with tables_changes as (

    select * from {{ref ('tables_changes')}}

),

tables_changes_desc as (

    select
        change_id,
        full_table_name,
        detected_at,
        change,

        case
            when change='table_added'
                then concat('The table "', full_table_name, '" was added')
            when change='table_removed'
                then concat('The table "', full_table_name, '" was removed')
            else 'no description'
        end as change_description,

        case
            when change='table_added'
                then to_varchar(table_schema)
            when change='table_removed'
                then to_varchar(table_schema)
            else null
        end as change_info

    from tables_changes

)

select * from tables_changes_desc
