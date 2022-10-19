{{
  config(
    materialized = 'view',
    enabled = target.type != 'databricks' and target.type != 'spark' | as_bool()
  )
}}

{% set configured_schemas = elementary.get_configured_schemas_from_graph() %}

with filtered_information_schema_tables as (

    {%- if configured_schemas | length > 0 -%}
        {%- set tables_from_info_schema_macro = context['elementary']['get_tables_from_information_schema'] -%}
        {{ elementary.run_query_macro_on_list(configured_schemas, tables_from_info_schema_macro) }}
    {%- else %}
        {{ elementary.empty_table([('full_table_name', 'string'), ('full_schema_name', 'string'), ('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string')]) }}
    {%- endif %}

)

select *
from filtered_information_schema_tables
where schema_name is not null
