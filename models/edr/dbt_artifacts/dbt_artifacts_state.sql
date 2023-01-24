{{
  config(
    materialized = 'view',
    bind=False,
    enabled =(target.type != 'databricks' and target.type != 'spark')
  )
}}

{% set hash_agg_expr %}
md5({{ elementary.listagg("artifact_hash", order_by_clause="order by artifact_hash") }})
{% endset %}

select
  (select {{ hash_agg_expr }} from {{ ref("dbt_models") }}) as dbt_models,
  (select {{ hash_agg_expr }} from {{ ref("dbt_tests") }}) as dbt_tests,
  (select {{ hash_agg_expr }} from {{ ref("dbt_sources") }}) as dbt_sources,
  (select {{ hash_agg_expr }} from {{ ref("dbt_snapshots") }}) as dbt_snapshots,
  (select {{ hash_agg_expr }} from {{ ref("dbt_exposures") }}) as dbt_exposures,
  (select {{ hash_agg_expr }} from {{ ref("dbt_metrics") }}) as dbt_metrics,
  (select {{ hash_agg_expr }} from {{ ref("dbt_seeds") }}) as dbt_seeds
