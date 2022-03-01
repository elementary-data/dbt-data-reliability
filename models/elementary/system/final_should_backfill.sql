with tables_config as (

    select * from {{ ref('final_tables_config') }}
    where columns_monitored = true
        and config_loaded_at = (select max(config_loaded_at) from {{ ref('final_tables_config') }})
),

columns_config as (

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
            else false end
        as should_backfill
    from tables_config as tab left join columns_config as col
        on (tab.full_table_name = col.full_table_name)
    group by 1,2

)

select * from should_backfill