{% snapshot source_tables_schemas_snapshot %}

{{
    config(
      target_database= target.database,
      target_schema= target.schema,
      unique_key='full_table_name',
      strategy='check',
      updated_at='updated_at',
      check_cols=['columns_schema'],
      invalidate_hard_deletes=true,
    )
}}

{% set monitored_full_table_name = get_monitored_full_table_names() %}
{% set monitored_dbs = get_monitored_dbs() %}

with monitored_tables_schemas as (
    {{ union_all_diff_dbs(monitored_dbs, get_schemas_snapshot_data) }}
),
final as (
  select
    full_table_name,
    database_name,
    table_schema,
    table_name,
    array_agg(object_construct('column_name', column_name, 'data_type', data_type, 'is_nullable', is_nullable)) within group (order by column_name) as columns_schema,
    {{ dbt_utils.current_timestamp() }} as updated_at
  from monitored_tables_schemas
  group by full_table_name, database_name, table_schema, table_name
)

select * from final

{% endsnapshot %}
