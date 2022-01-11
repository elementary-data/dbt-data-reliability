with schemas_snapshot as (

    select * from {{ ref('source_tables_columns_snapshot') }}
),

schemas_order as (

    select *,
        row_number() over (partition by full_table_name order by dbt_updated_at desc) as schema_order
    from schemas_snapshot

),

current_schemas as (

    select *
    from schemas_order
    where schema_order = 1

),

previous_schemas as (

    select *
    from schemas_order
    where schema_order = 2

),

final as (

    select
        cur.full_table_name,
        cur.columns_schema as current_schema,
        pre.columns_schema as previous_schema,
        cur.dbt_updated_at,
        cur.dbt_valid_from,
        cur.dbt_valid_to
    from current_schemas cur
    left join previous_schemas pre
        on (cur.full_table_name = pre.full_table_name)

)

select * from final
