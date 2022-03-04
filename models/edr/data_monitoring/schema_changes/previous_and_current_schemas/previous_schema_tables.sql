{{
  config(
    materialized='ephemeral'
  )
}}

with tables_snapshot as (

    select * from {{ ref('schema_tables_snapshot') }}
),

previous_run_time as (

    select detected_at
    from tables_snapshot
    group by detected_at
    order by detected_at desc
    limit 1 offset 1

),

previous_schema as (

    select
        full_schema_name,
        full_table_name,
        is_new,
        detected_at
    from tables_snapshot
    where detected_at = (select detected_at from previous_run_time)

)

select * from previous_schema
