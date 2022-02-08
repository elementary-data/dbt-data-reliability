with active_tables as (

    select * from {{ ref('active_tables_and_views') }}

),

access_history as (

    select * from {{ ref('stg_access_history') }}
    where query_start_time > (current_date - {{ var('account_usage_days_back_limit') }})::timestamp

),

active_tables_edges as (

        select
            distinct
            modified_table_name as target_table,
            direct_access_table_name as source_table
        from
            access_history join active_tables as active_targets
                on (access_history.modified_table_name = active_targets.full_table_name)
            join active_tables as active_sources
                on (access_history.direct_access_table_name = active_sources.full_table_name)
        where target_table is not null and target_table != source_table

),

upstream_lineage as (

    with recursive rec_upstream_lineage (target_table, source_table, depth) as
    (
        select
            target_table,
            source_table,
            0 as depth
        from active_tables_edges
            union all
        select
            cte.target_table,
            edg.source_table,
            depth+1 as depth
        from active_tables_edges as edg join rec_upstream_lineage as cte
            on (edg.target_table=cte.source_table)
        where depth < {{ var('lineage_max_depth') }}
    )

    select
        target_table,
        listagg(distinct source_table, ', ') as upstream_tables
    from rec_upstream_lineage
    group by target_table

    ),

downstream_lineage as (

    with recursive rec_downstream_lineage (source_table, target_table, depth) as
    (
        select
            source_table,
            target_table,
            0 as depth
        from active_tables_edges
            union all
        select
            cte.source_table,
            edg.target_table,
            depth+1 as depth
        from active_tables_edges as edg join rec_downstream_lineage as cte
            on (cte.target_table=edg.source_table)
        where depth < {{ var('lineage_max_depth') }}
    )

    select
        source_table,
        listagg(distinct target_table, ', ') as downstream_tables
    from rec_downstream_lineage
    group by source_table

    ),

table_lineage as (

    select
        coalesce(up.target_table, down.source_table) as table_name,
        upstream_tables,
        downstream_tables
    from downstream_lineage as down full outer join upstream_lineage as up
        on (up.target_table = down.source_table)

)

select * from table_lineage

