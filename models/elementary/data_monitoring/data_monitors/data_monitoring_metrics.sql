{{
  config(
    materialized='incremental',
    unique_key = 'id',
    post_hook = "{{ monitors_run_end() }}"
  )
}}

-- depends on: {{ ref('init_data_monitors_thread_1') }}
-- depends on: {{ ref('init_data_monitors_thread_2') }}
-- depends on: {{ ref('init_data_monitors_thread_3') }}
-- depends on: {{ ref('init_data_monitors_thread_4') }}

with monitors_run as (

    select * from {{ ref('run_data_monitors_thread_1') }}
    where not (full_table_name is null and metric_name is null and metric_value is null)
    union all
    select * from {{ ref('run_data_monitors_thread_2') }}
    where not (full_table_name is null and metric_name is null and metric_value is null)
    union all
    select * from {{ ref('run_data_monitors_thread_3') }}
    where not (full_table_name is null and metric_name is null and metric_value is null)
    union all
    select * from {{ ref('run_data_monitors_thread_4') }}
    where not (full_table_name is null and metric_name is null and metric_value is null)

),

final_metrics as (

    select
        {{ dbt_utils.surrogate_key([
            'full_table_name',
            'column_name',
            'metric_name',
            'timeframe_start',
            'timeframe_end'
        ]) }} as id,
        full_table_name,
        column_name,
        metric_name,
        metric_value,
        timeframe_start,
        timeframe_end,
        timeframe_duration_hours,
        {{- dbt_utils.current_timestamp_in_utc() -}} as updated_at
    from monitors_run
)

select
    id,
    full_table_name,
    column_name,
    metric_name,
    metric_value,
    timeframe_start,
    timeframe_end,
    timeframe_duration_hours,
    max(updated_at) as updated_at
from final_metrics
group by 1,2,3,4,5,6,7,8