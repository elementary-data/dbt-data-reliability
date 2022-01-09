with schemas_order as (

    select
    full_table_name,
    columns_schema,
    row_number() over (partition by full_table_name order by dbt_updated_at desc) as schema_order,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
    from {{ ref('source_tables_schemas_snapshot') }}
    order by full_table_name
 ),

current_schemas as (
    select
    full_table_name,
    columns_schema as current_schema,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to,
    schema_order
    from schemas_order
    where schema_order=1
),

previous_schemas as (
    select full_table_name,
    columns_schema as previous_schema,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to,
    schema_order
    from schemas_order
    where schema_order=2
),

final as (
    select
    full_table_name,
    current_schema,
    previous_schema,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
    from current_schemas c left join previous_schemas p using (full_table_name)
)

select * from final
