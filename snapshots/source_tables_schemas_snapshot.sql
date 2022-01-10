{% snapshot source_tables_schemas_snapshot %}

{{
  config (
    target_database= target.database,
    target_schema= target.schema,
    unique_key='full_table_name',
    strategy='check',
    check_cols=['columns_schema'],
    invalidate_hard_deletes=true
    )
}}

{% set monitored_dbs = get_monitored_dbs() %}

with monitored_dbs_schemas as (

    {{ union_schemas_for_snapshot() }}

),

final as (

    select
        full_table_name,
        database_name,
        schema_name,
        table_name,

        array_agg(object_construct('column_name', column_name, 'data_type', data_type))
            within group (order by column_name)
        as columns_schema

    from monitored_dbs_schemas
    group by 1, 2, 3, 4

)

select * from final

{% endsnapshot %}
