{{
  config(
    materialized = 'view',
    bind=False
  )
}}

with data_monitoring_metrics as (

    select * from {{ ref('data_monitoring_metrics') }}

),

max_bucket_end as (

    select full_table_name,
           column_name,
           metric_name,
           metric_properties,
           max(bucket_end) as last_bucket_end,
           min(bucket_end) as first_bucket_end
    from data_monitoring_metrics
    group by 1,2,3,4

)

select * from max_bucket_end