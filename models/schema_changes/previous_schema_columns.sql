{{
  config(
    materialized='ephemeral'
  )
}}

with columns_snapshot as (

    select * from {{ ref('columns_snapshot') }}
),

previous_run_time as (

    select detected_at
    from columns_snapshot
    group by detected_at
    order by detected_at desc
    limit 1 offset 1

),

previous_run as (

    select
        full_table_name,
        column_name,
        data_type,
        is_new,
        detected_at
    from columns_snapshot
    where detected_at = (select detected_at from previous_run_time)

)

select * from previous_run
