{{
  config(
    materialized = 'view',
    bind=False
  )
}}

{% set artifact_models = [
  "dbt_models",
  "dbt_tests",
  "dbt_sources",
  "dbt_snapshots",
  "dbt_metrics",
  "dbt_exposures",
  "dbt_seeds",
] %}

{% for artifact_model in artifact_models %}
select
  '{{ artifact_model }}' as artifacts_model,
   artifact_hash
from {{ ref(artifact_model) }}
order by artifact_hash
{% if not loop.last %} union all {% endif %}
{% endfor %}
