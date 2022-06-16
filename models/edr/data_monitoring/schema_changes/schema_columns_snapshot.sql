{{
  config(
    materialized='incremental',
    unique_key = 'column_state_id'
  )
}}


with information_schema_columns as (

    select * from {{ ref('filtered_information_schema_columns') }}

),

columns_snapshot as (

    select
        full_table_name,
        database_name,
        schema_name,
        table_name,
        column_name,
        cast(data_type as {{ dbt_utils.type_string() }}) as data_type,
        {{ elementary.run_start_column() }} as detected_at,

        {% if is_incremental() %}
            {%- set known_columns_query %}
                select full_column_name from {{ this }}
                where detected_at = (select max(detected_at) from {{ this }})
            {% endset %}
            {%- set known_columns = elementary.result_column_to_list(known_columns_query) %}

            {%- set known_tables_query %}
                select distinct full_table_name from {{ this }}
                where detected_at = (select max(detected_at) from {{ this }})
            {% endset %}
            {%- set known_tables = elementary.result_column_to_list(known_tables_query) %}

            case when
                {{ elementary.full_column_name() }} not in {{ elementary.strings_list_to_tuple(known_columns) }}
                and full_table_name in {{ elementary.strings_list_to_tuple(known_tables) }}
            then true
            else false end
            as is_new
        {% else %}
            false as is_new
        {% endif %}
    from information_schema_columns

),

columns_snapshot_with_id as (

    select
        {{ dbt_utils.surrogate_key([
          'full_table_name',
          'column_name',
          'data_type'
        ]) }} as column_state_id,
        {{ elementary.full_column_name() }} as full_column_name,
        full_table_name,
        column_name,
        data_type,
        is_new,
        max(detected_at) as detected_at
    from columns_snapshot
    group by 1,2,3,4,5,6

)

select
    {{ elementary.cast_as_string('column_state_id') }} as column_state_id,
    {{ elementary.cast_as_string('full_column_name') }} as full_column_name,
    {{ elementary.cast_as_string('full_table_name') }} as full_table_name,
    {{ elementary.cast_as_string('column_name') }} as column_name,
    {{ elementary.cast_as_string('data_type') }} as data_type,
    {{ elementary.cast_as_bool('is_new') }} as is_new,
    {{ elementary.cast_as_timestamp('detected_at') }} as detected_at
from columns_snapshot_with_id