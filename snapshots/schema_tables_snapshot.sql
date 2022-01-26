{% snapshot schema_tables_snapshot %}

{{
  config (
    target_database= target.database,
    target_schema= target.schema,
    unique_key='full_schema_name',
    strategy='check',
    check_cols=['tables_in_schema'],
    invalidate_hard_deletes=True
    )
}}

{% set configured_schemas = get_configured_schemas() %}

with filtered_information_schema_tables as (

    {% if configured_schemas != [] %}
        {{ query_different_schemas(get_tables_from_information_schema, configured_schemas) }}
    {% else %}
        {{ empty_table([('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string')]) }}
    {% endif %}
),

final as (

    select
        {{ full_schema_name() }} as full_schema_name,
        array_agg(table_name)
            within group (order by table_name)
        as tables_in_schema

    from filtered_information_schema_tables
    where schema_name is not null
    group by 1

)

select * from final

{% endsnapshot %}
