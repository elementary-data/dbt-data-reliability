{{
  config(
    materialized = 'view',
    bind=False,
    unique_key='unique_id',
  )
}}

with dbt_exposures as (
  select * from {{ ref('dbt_exposures') }}
),

elementary_exposures as (
  select * from {{ ref('elementary_exposures') }}
)

{# Union without duplicates where elementary_exposures has prio #}
select * from dbt_exposures where not exists (select 1 from elementary_exposures where dbt_exposures.unique_id = elementary_exposures.unique_id)
{{ elementary.sql_union_distinct() }} select * from elementary_exposures
