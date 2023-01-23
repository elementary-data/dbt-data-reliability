{{
  config(
    materialized = 'view',
    bind=False
  )
}}

select
  (select md5({{ dbt.listagg("artifact_hash", order_by_clause="order by artifact_hash") }}) from {{ ref("dbt_models") }}) as dbt_models,
  (select md5({{ dbt.listagg("artifact_hash", order_by_clause="order by artifact_hash") }}) from {{ ref("dbt_tests") }}) as dbt_tests,
  (select md5({{ dbt.listagg("artifact_hash", order_by_clause="order by artifact_hash") }}) from {{ ref("dbt_sources") }}) as dbt_sources,
  (select md5({{ dbt.listagg("artifact_hash", order_by_clause="order by artifact_hash") }}) from {{ ref("dbt_snapshots") }}) as dbt_snapshots,
  (select md5({{ dbt.listagg("artifact_hash", order_by_clause="order by artifact_hash") }}) from {{ ref("dbt_exposures") }}) as dbt_exposures,
  (select md5({{ dbt.listagg("artifact_hash", order_by_clause="order by artifact_hash") }}) from {{ ref("dbt_metrics") }}) as dbt_metrics,
  (select md5({{ dbt.listagg("artifact_hash", order_by_clause="order by artifact_hash") }}) from {{ ref("dbt_seeds") }}) as dbt_seeds
