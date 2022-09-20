{{
  config(
    materialized = 'view',
    enabled = target.type != 'databricks' and target.type != 'spark' | as_bool()
  )
}}

{% set configured_schemas = elementary.get_configured_schemas_from_graph() %}

with filtered_information_schema_columns as (

    {%- if configured_schemas | length > 0 -%}
        {%- set columns_from_info_schema_macro = context['elementary']['get_columns_from_information_schema'] %}
        {{ elementary.run_query_macro_on_list(configured_schemas, columns_from_info_schema_macro) }}
    {%- else %}
        {{ elementary.empty_table([('full_table_name', 'string'), ('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string'), ('column_name', 'string'), ('data_type', 'string')]) }}
    {%- endif %}

)

select *
from filtered_information_schema_columns
where full_table_name is not null