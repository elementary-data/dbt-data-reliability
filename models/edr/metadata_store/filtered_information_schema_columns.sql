{{
  config(
    materialized = 'view',
    enabled = target.type != 'databricks' and target.type != 'spark' | as_bool()
  )
}}

{%- if target.type != 'databricks' and target.type != 'spark' %}
    {% set configured_schemas = elementary.get_configured_schemas_from_graph() %}

with filtered_information_schema_columns as (
    {{ elementary.get_columns_by_schemas(configured_schemas) }}
)

select *
from filtered_information_schema_columns
where full_table_name is not null

{%- else %}
    {{ elementary.no_results_query() }}
{%- endif %}
