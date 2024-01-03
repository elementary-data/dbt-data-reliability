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
  "dbt_columns",
] %}

{% for artifact_model in artifact_models %}
select
  '{{ artifact_model }}' as artifacts_model,
   metadata_hash
from {{ ref(artifact_model) }}
{% if not loop.last %} union all {% endif %}
{% endfor %}
order by metadata_hash
