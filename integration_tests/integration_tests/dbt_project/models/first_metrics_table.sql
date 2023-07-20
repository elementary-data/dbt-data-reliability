{{ config(materialized="incremental") }}

select * from {{ source("test_data", "first_metrics_table_seed") }}
