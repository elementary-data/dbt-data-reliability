{{
  config(
    materialized='ephemeral'
  )
}}

with tables_snapshot as (

    select * from {{ ref('tables_snapshot') }}
),

this_run_time as (

    select detected_at
    from tables_snapshot
    order by detected_at desc
    limit 1

),

current_tables as (

    select
        full_schema_name,
        full_table_name,
        is_new,
        detected_at
    from tables_snapshot
    where detected_at = (select detected_at from this_run_time)

)

select * from current_tables