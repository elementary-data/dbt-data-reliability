{%- set max_timeframe_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00")~"'" %}
{%- set last_run_limit = "'"~ elementary.min_start_time(get_config_var('days_back'), max_timeframe_end)~"'" %}
{%- set days_subtract = '-' ~ get_config_var('days_back') %}
{%- set min_buckets_subtract = '-' ~ get_config_var('min_buckets_per_run') %}

-- depends_on: {{ ref('elementary_runs') }}

with tables_config as (

    select * from {{ ref('final_tables_config') }}
    where (table_monitored = true or columns_monitored = true)
        and config_loaded_at = (select max(config_loaded_at) from {{ ref('final_tables_config') }})

),

columns_config_should_backfill_true as (

    select * from {{ ref('final_columns_config') }}
    where config_loaded_at = (select max(config_loaded_at) from {{ ref('final_columns_config') }})
        and should_backfill = true

),

should_backfill as (

    select
        tab.full_table_name,
        case
            when tab.should_backfill = true then true
            when col.should_backfill = true then true
            else false
        end as should_backfill,
        case
            when tab.should_backfill = true then {{ dbt_utils.dateadd('day', days_subtract, max_timeframe_end) }}
            when col.should_backfill = true then {{ dbt_utils.dateadd('day', days_subtract, max_timeframe_end ) }}
            else {{ dbt_utils.dateadd('day', min_buckets_subtract, last_run_limit) }}
        end as min_timeframe_start
    from tables_config as tab left join columns_config_should_backfill_true as col
        on (tab.full_table_name = col.full_table_name)
    group by 1,2,3

)

select * from should_backfill
