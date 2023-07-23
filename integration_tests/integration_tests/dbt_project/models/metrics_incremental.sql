{{ config(materialized="incremental") }}

select * from {{ source("test_data", "second_metrics_table_seed") }}