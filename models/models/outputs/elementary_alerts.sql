{{
  config(
    materialized = 'incremental',
    unique_key = 'change_id'
  )
}}


with tables_for_alerts as (

    select * from {{ ref('config_alerts__tables') }}
    where alert_on_schema_changes = true

),

columns_for_alerts as (

    select * from {{ ref('config_alerts__columns') }}
    where alert_on_schema_changes = true

),

tables_changes as (

    select * from {{ ref('tables_changes_description') }}

),

columns_changes as (

    select * from {{ ref('columns_changes_description') }}

),

alerts_tables_changes as (

    select
        change_id as alert_id,
        detected_at,
        table_changes.full_table_name,
        'table_schema_change' as alert_type,
        change as alert_reason,
        change_description as alert_reason_value,
        array_construct('change_info') as alert_details_keys,
        array_construct(change_info) as alert_details_values
    from tables_for_alerts
        left join tables_changes
        on (tables_for_alerts.full_table_name = table_changes.full_table_name)

),

alerts_columns_changes as (

    select
        change_id as alert_id,
        detected_at,
        columns_changes.full_table_name,
        'table_schema_change' as alert_type,
        change as alert_reason,
        change_description as alert_reason_value,
        array_construct('change_info') as alert_details_keys,
        array_construct(change_info) as alert_details_values
    from columns_for_alerts
        left join columns_changes
        on (columns_for_alerts.full_table_name = columns_changes.full_table_name
        and columns_for_alerts.column_name = columns_changes.column_name)

),

union_alerts as (

    select * from alerts_tables_changes
        union all
    select * from alerts_columns_changes
)

select * from union_alerts
