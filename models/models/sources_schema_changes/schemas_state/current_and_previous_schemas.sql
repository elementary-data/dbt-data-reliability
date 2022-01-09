with schemas_snapshot as (

    select
        *
    from {{ ref('source_tables_schemas_snapshot') }}
),

schemas_order as (

    select
        *,
        row_number() over (partition by full_table_name order by dbt_updated_at desc) as schema_order
    from schemas_snapshot
),

current_schemas as (
    select
        *
    from schemas_order
    where schema_order=1
),

previous_schemas as (
    select
        *
    from schemas_order
    where schema_order=2
),

final as (
    select
        full_table_name,
        current_schemas.current_schema,
        previous_schemas.previous_schema,
        current_schemas.dbt_updated_at,
        current_schemas.dbt_valid_from,
        current_schemas.dbt_valid_to
    from current_schemas left join previous_schemas on (full_table_name)
)

select * from final
