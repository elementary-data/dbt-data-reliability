{% snapshot source_schemas_tables_snapshot %}

{{
  config (
    target_database= target.database,
    target_schema= target.schema,
    unique_key='full_schema_name',
    strategy='check',
    check_cols=['tables_in_schema'],
    invalidate_hard_deletes=true
    )
}}

{% set monitored_dbs = get_monitored_dbs() %}

with monitored_dbs_schemas as (

    {{ union_schemas_for_snapshot(get_tables_from_information_schema) }}

),

final as (

    select
        concat(database_name,'.',schema_name) as full_schema_name,
        database_name,
        schema_name,

        array_agg(distinct table_name)
            within group (order by table_name)
        as tables_in_schema

    from monitored_dbs_schemas
    group by 1, 2, 3

)

select * from final

{% endsnapshot %}
