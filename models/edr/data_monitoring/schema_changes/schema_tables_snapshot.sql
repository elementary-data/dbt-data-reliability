{{
  config(
    materialized='incremental',
    unique_key = 'table_state_id'
  )
}}

with information_schema_tables as (

    select * from {{ ref('filtered_information_schema_tables') }}

),

schema_tables as (

    select
        full_schema_name,
        table_name,
        full_table_name,
        {{ elementary.run_start_column() }} as detected_at,

        {% if is_incremental() %}
            {%- set known_tables_query %}
                select full_table_name from {{ this }}
                where detected_at = (select max(detected_at) from {{ this }})
            {% endset %}
            {%- set known_tables = elementary.result_column_to_list(known_tables_query) %}

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

    from information_schema_tables

),

schema_tables_with_id as (

    select
        {{ dbt_utils.surrogate_key([
          'full_table_name'
        ]) }} as table_state_id,
        full_schema_name,
        full_table_name,
        is_new,
        max(detected_at) as detected_at
    from schema_tables
    group by 1,2,3,4

)

select
    {{ elementary.cast_as_string('table_state_id') }} as table_state_id,
    {{ elementary.cast_as_string('full_schema_name') }} as full_schema_name,
    {{ elementary.cast_as_string('full_table_name') }} as full_table_name,
    {{ elementary.cast_as_bool('is_new') }} as is_new,
    {{ elementary.cast_as_timestamp('detected_at') }} as detected_at
from schema_tables_with_id