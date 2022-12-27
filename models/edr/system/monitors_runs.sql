{{
  config(
    materialized = 'view' if target.type != 'duckdb' else 'table',
    bind=False
  )
}}

with data_monitoring_metrics_cte as (

    select * from {{ ref('data_monitoring_metrics') }}

),

max_bucket_end as (

    select full_table_name,
           column_name,
           metric_name,
           max(bucket_end) as last_bucket_end
    from data_monitoring_metrics_cte
    group by 1,2,3

)

select * from max_bucket_end