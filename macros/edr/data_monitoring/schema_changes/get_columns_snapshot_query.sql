{% macro get_columns_snapshot_query(full_table_name) %}

    {%- set known_columns_query %}
        select full_column_name from {{ ref('schema_columns_snapshot') }}
        where detected_at = (select max(detected_at) from {{ ref('schema_columns_snapshot') }} where lower(full_table_name) = lower('{{ full_table_name }}'))
        and lower(full_table_name) = lower('{{ full_table_name }}')
    {% endset %}

    {%- set known_tables_query %}
        select distinct full_table_name from {{ ref('schema_columns_snapshot') }}
        where detected_at = (select max(detected_at) from {{ ref('schema_columns_snapshot') }} where lower(full_table_name) = lower('{{ full_table_name }}'))
        and lower(full_table_name) = lower('{{ full_table_name }}')
    {% endset %}


    with information_schema_columns as (

        select * from {{ ref('filtered_information_schema_columns') }}
        where lower(full_table_name) = lower('{{ full_table_name }}')

    ),

    columns_snapshot as (

        select
            full_table_name,
            database_name,
            schema_name,
            table_name,
            column_name,
            cast(data_type as {{ elementary.type_string() }}) as data_type,
            {{ elementary.datetime_now_utc_as_timestamp_column() }} as detected_at,
            case when
                    {{ elementary.full_column_name() }} not in ({{ known_columns_query }})
                    and full_table_name in ({{ known_tables_query }})
                then true
                else false
            end as is_new
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
            detected_at
        from columns_snapshot
        group by 1,2,3,4,5,6,7

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

{%- endmacro %}