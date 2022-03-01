{{
  config(
    materialized='incremental',
    unique_key = 'table_state_id'
  )
}}


with tables as (

    select
        {{ elementary.full_schema_name() }} as full_schema_name,
        table_name,
        full_table_name,
        {{ elementary.run_start_column() }} as detected_at,

        {% if is_incremental() %}
            {%- set known_tables_query %}
                select full_table_name from {{ this }}
                where detected_at = (select max(detected_at) from {{ this }})
            {% endset %}
            {%- set known_tables = result_column_to_list(known_tables_query) %}

            {%- set known_schemas_query %}
                select distinct full_schema_name from {{ this }}
                where detected_at = (select max(detected_at) from {{ this }})
            {% endset %}
            {%- set known_schemas = elementary.result_column_to_list(known_schemas_query) %}

            case when
                full_table_name not in {{ elementary.strings_list_to_tuple(known_tables) }}
                and full_schema_name in {{ elementary.strings_list_to_tuple(known_schemas) }}
            then true
            else false end
            as is_new
        {% else %}
            false as is_new
        {% endif %}

    from {{ ref('information_schema_tables') }}

)

select
    {{ dbt_utils.surrogate_key([
      'full_schema_name',
      'table_name'
    ]) }} as table_state_id,
    full_schema_name,
    full_table_name,
    is_new,
    max(detected_at) as detected_at
from tables
group by 1,2,3,4