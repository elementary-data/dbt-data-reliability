{{ config(materialized="table") }}

select * from {{ source("test_data", "second_metrics_table_seed") }}
