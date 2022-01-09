{{
    config(
        materialized = 'incremental',
        unique_key = 'change_id'
    )
}}

{% set monitored_tables = get_monitored_full_table_names() %}
{% set latest_update %}
    (select max(dbt_updated_at) from {{ ref('current_and_previous_schemas')}})
{% endset %}

with all_monitored_tables as (
    select * from {{ ref('current_and_previous_schemas')}}
    where full_table_name in {{ list_to_tuple(monitored_tables) }}
),
removed_tables as (
    select full_table_name,
    'table_removed' as change,
    current_schema,
    dbt_updated_at as detected_at
    from all_monitored_tables
    where dbt_valid_to = {{latest_update}}
)
select
{{ dbt_utils.surrogate_key([
  'full_table_name',
  'change'
  ]) }} as change_id,
  full_table_name,
  change,
  current_schema,
  detected_at
 from removed_tables
 {% if is_incremental() %}
   where detected_at > (select max(detected_at) from {{ this }})
 {% endif %}
