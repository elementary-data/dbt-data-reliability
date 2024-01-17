{% macro get_columns_snapshot_query(model_relation, full_table_name) %}
    {%- set schema_columns_snapshot_relation = elementary.get_elementary_relation('schema_columns_snapshot') %}
    {%- set known_columns_query %}
        select full_column_name from {{ schema_columns_snapshot_relation }}
        where detected_at = (select max(detected_at) from {{ schema_columns_snapshot_relation }} where lower(full_table_name) = lower('{{ full_table_name }}'))
        and lower(full_table_name) = lower('{{ full_table_name }}')
    {% endset %}

    {%- set known_tables_query %}
        select distinct full_table_name from {{ schema_columns_snapshot_relation }}
        where detected_at = (select max(detected_at) from {{ schema_columns_snapshot_relation }} where lower(full_table_name) = lower('{{ full_table_name }}'))
        and lower(full_table_name) = lower('{{ full_table_name }}')
    {% endset %}

    {% set columns = adapter.get_columns_in_relation(model_relation) %}

    with table_info as (
        select
            {{ elementary.edr_cast_as_string(elementary.edr_quote(full_table_name)) }} as full_table_name,
            {{ elementary.edr_cast_as_string(elementary.edr_quote(model_relation.database)) }} as database_name,
            {{ elementary.edr_cast_as_string(elementary.edr_quote(model_relation.schema)) }} as schema_name,
            {{ elementary.edr_cast_as_string(elementary.edr_quote(model_relation.identifier)) }} as table_name,
            {{ elementary.datetime_now_utc_as_timestamp_column() }} as detected_at
    ),

    columns_info as (
        select
            full_table_name,
            database_name,
            schema_name,
            table_name,
            column_name,
            data_type,
            detected_at
        from table_info
        cross join
            (
                {% for column in columns %}
                    select
                        {{ elementary.edr_cast_as_string(elementary.edr_quote(column.name)) }} as column_name,
                        {{ elementary.edr_cast_as_string(elementary.edr_quote(elementary.get_normalized_data_type(elementary.get_column_data_type(column)))) }} as data_type
                    {% if not loop.last %}
                        union all
                    {% endif %}
                {% endfor %}
            ) rt
    ),

    columns_snapshot as (
        select
            full_table_name,
            database_name,
            schema_name,
            table_name,
            column_name,
            data_type,
            detected_at,
            case when
                    {{ elementary.full_column_name() }} not in ({{ known_columns_query }})
                    and full_table_name in ({{ known_tables_query }})
                then true
                else false
            end as is_new
        from columns_info
    ),

    columns_snapshot_with_id as (
        select
            {{ elementary.generate_surrogate_key([
              'full_table_name',
              'column_name',
              'data_type',
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
        {{ elementary.edr_cast_as_string('column_state_id') }} as column_state_id,
        {{ elementary.edr_cast_as_string('full_column_name') }} as full_column_name,
        {{ elementary.edr_cast_as_string('full_table_name') }} as full_table_name,
        {{ elementary.edr_cast_as_string('column_name') }} as column_name,
        {{ elementary.edr_cast_as_string('data_type') }} as data_type,
        {{ elementary.edr_cast_as_bool('is_new') }} as is_new,
        {{ elementary.edr_cast_as_timestamp('detected_at') }} as detected_at
    from columns_snapshot_with_id

{%- endmacro %}