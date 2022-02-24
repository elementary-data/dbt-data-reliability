{{
  config(
    materialized = 'incremental',
    unique_key = 'config_id'
  )
}}


with columns_config as (

    select * from {{ var('columns_config') }}

),

information_schema_columns as (

    select * from {{ ref('information_schema_columns') }}

),

config_existing_columns as (

    select
        {{ dbt_utils.surrogate_key([
            'config.database_name', 'config.schema_name', 'config.table_name', 'config.column_name', 'config.monitors',
        ]) }} as config_id,
        {{ full_table_name('config') }} as full_table_name,
        upper(config.database_name) as database_name,
        upper(config.schema_name) as schema_name,
        upper(config.table_name) as table_name,
        upper(config.column_name) as column_name,
        info_schema.data_type,
        monitored,
        monitors,
        {{ run_start_column() }} as config_loaded_at
    from
        information_schema_columns as info_schema join columns_config as config
        on (upper(info_schema.database_name) = upper(config.database_name)
            and upper(info_schema.schema_name) = upper(config.schema_name)
            and upper(info_schema.table_name) = upper(config.table_name)
            and upper(info_schema.column_name) = upper(config.column_name))
),

final as (

    select
        config_id,
        full_table_name,
        database_name,
        schema_name,
        table_name,
        column_name,
        data_type,
        monitored,
        monitors,

        {% if is_incremental() %}
            {%- set active_configs_query %}
                select config_id from {{ this }}
                where config_loaded_at = (select max(config_loaded_at) from {{ this }})
                and monitored = true
            {% endset %}
            {%- set active_configs = result_column_to_list(active_configs_query) %}

            case when
                config_id not in {{ strings_list_to_tuple(active_configs) }}
            then true
            else false end
            as should_backfill,
        {% else %}
            true as should_backfill,
        {% endif %}

        max(config_loaded_at) as config_loaded_at

    from config_existing_columns
    group by 1,2,3,4,5,6,7,8,9,10
)

select *
from final