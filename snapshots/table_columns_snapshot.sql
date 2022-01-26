{% snapshot table_columns_snapshot %}

{{
  config (
    target_database= target.database,
    target_schema= target.schema,
    unique_key='full_table_name',
    strategy='check',
    check_cols=['columns_schema'],
    invalidate_hard_deletes=True
    )
}}

{% set configured_schemas = get_configured_schemas() %}

with filtered_information_schema_columns as (

    {% if configured_schemas != [] %}
        {{ query_different_schemas(get_columns_from_information_schema, configured_schemas) }}
    {% else %}
        {{ empty_table([('full_table_name', 'string'), ('column_name', 'string'), ('data_type', 'string')]) }}
    {% endif %}

),

final as (

    select
        full_table_name,
        array_agg(object_construct('column_name', column_name, 'data_type', data_type))
            within group (order by column_name)
        as columns_schema

    from filtered_information_schema_columns
    where full_table_name is not null
    group by 1

)

select * from final

{% endsnapshot %}
