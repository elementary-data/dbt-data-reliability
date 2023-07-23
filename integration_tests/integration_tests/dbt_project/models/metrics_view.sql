{{ config(materialized="view") }}

select * from {{ source("test_data", "first_metrics_table_seed") }}
union all
select * from {{ source("test_data", "second_metrics_table_seed") }}
