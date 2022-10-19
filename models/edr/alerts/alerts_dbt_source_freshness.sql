{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with results as (
  select * from {{ ref('dbt_source_freshness_results') }}
),

sources as (
  select * from {{ ref('dbt_sources') }}
)

select
  results.source_freshness_execution_id as alert_id,
  results.max_loaded_at,
  results.snapshotted_at,
  results.generated_at as detected_at,
  results.max_loaded_at_time_ago_in_s,
  results.status,
  results.error,
  sources.unique_id,
  sources.database_name,
  sources.schema_name,
  sources.source_name,
  sources.identifier,
  sources.freshness_error_after,
  sources.freshness_warn_after,
  sources.freshness_filter,
  sources.tags,
  sources.meta,
  sources.owner,
  sources.package_name,
  sources.path
from results
join sources on results.unique_id = sources.unique_id
where {{ not elementary.get_config_var('disable_source_freshness_alerts') }} and lower(status) != 'pass'
