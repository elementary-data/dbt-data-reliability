{{
  config(
    materialized = 'view',
    bind=False
  )
}}

select
  (select md5({{ elementary.cast_as_string("array_agg(hash)") }}) from {{ ref("dbt_models") }}) as models_hash,
  (select md5({{ elementary.cast_as_string("array_agg(hash)") }}) from {{ ref("dbt_tests") }}) as tests_hash,
  (select md5({{ elementary.cast_as_string("array_agg(hash)") }}) from {{ ref("dbt_sources") }}) as sources_hash,
  (select md5({{ elementary.cast_as_string("array_agg(hash)") }}) from {{ ref("dbt_snapshots") }}) as snapshots_hash,
  (select md5({{ elementary.cast_as_string("array_agg(hash)") }}) from {{ ref("dbt_exposures") }}) as exposures_hash,
  (select md5({{ elementary.cast_as_string("array_agg(hash)") }}) from {{ ref("dbt_metrics") }}) as metrics_hash
