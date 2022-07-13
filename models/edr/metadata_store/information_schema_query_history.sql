{{
  config(
    materialized = 'view',
    enabled = target.type == 'snowflake' | as_bool()
  )
}}

{%- set models_schemas = elementary.get_models_schemas_from_graph() -%}

with information_schema_query_history as (

    {%- if models_schemas | length > 0 -%}
        {%- set tables_from_info_schema_macro = context['elementary']['get_query_history_from_information_schema'] -%}
        {{ elementary.run_query_macro_on_list(models_schemas, tables_from_info_schema_macro) }}
    {%- else %}
        {{ elementary.empty_table([('full_table_name', 'string'), ('full_schema_name', 'string'), ('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string')]) }}
    {%- endif %}

)

select *
from information_schema_query_history