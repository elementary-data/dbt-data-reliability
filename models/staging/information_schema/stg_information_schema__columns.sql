{% set monitored_dbs = get_monitored_dbs() %}

with monitored_dbs_schemas as (
    {{ union_all_diff_dbs(monitored_dbs, get_schemas_snapshot_data) }}
)

select * from monitored_dbs_schemas