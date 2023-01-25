{{
  config(
    materialized = 'view',
    bind=False
  )
}}

{% set hash_agg_expr %}
md5({{ elementary.listagg("artifact_hash") }})
{% endset %}




select
  (select {{ hash_agg_expr }} from ({{ elementary.get_ordered_artifact_hash_query("dbt_models") }}) results) as dbt_models,
  (select {{ hash_agg_expr }} from ({{ elementary.get_ordered_artifact_hash_query("dbt_tests") }}) results) as dbt_tests,
  (select {{ hash_agg_expr }} from ({{ elementary.get_ordered_artifact_hash_query("dbt_sources") }}) results) as dbt_sources,
  (select {{ hash_agg_expr }} from ({{ elementary.get_ordered_artifact_hash_query("dbt_snapshots") }}) results) as dbt_snapshots,
  (select {{ hash_agg_expr }} from ({{ elementary.get_ordered_artifact_hash_query("dbt_exposures") }}) results) as dbt_exposures,
  (select {{ hash_agg_expr }} from ({{ elementary.get_ordered_artifact_hash_query("dbt_metrics") }}) results) as dbt_metrics,
  (select {{ hash_agg_expr }} from ({{ elementary.get_ordered_artifact_hash_query("dbt_seeds") }}) results) as dbt_seeds
