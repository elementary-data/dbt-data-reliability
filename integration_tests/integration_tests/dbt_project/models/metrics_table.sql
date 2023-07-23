{{ config(materialized="table") }}

select * from {{ source("test_data", "first_metrics_table_seed") }}
