{{
    config(
        materialized = 'incremental',
        unique_key = 'change_id'
    )
}}


{% set latest_update %}
    (select max(dbt_updated_at) from {{ ref('current_and_previous_schemas')}})
{% endset %}

with all_tables as (
    select * from {{ ref('current_and_previous_schemas')}}
),

new_tables as (
    select
    full_table_name,
    'table_added' as change,
    current_schema,
    dbt_updated_at as detected_at
    from all_tables
    where dbt_valid_from = {{latest_update}}
),
removed_tables as (
    select full_table_name,
    'table_removed' as change,
    current_schema,
    dbt_updated_at as detected_at
    from all_tables
    where dbt_valid_to = {{latest_update}}
),

tables_changes as (
    select * from new_tables
    union all
    select * from removed_tables
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
 from tables_changes
 {% if is_incremental() %}
    where detected_at > (select max(detected_at) from {{ this }})
  {% endif %}
