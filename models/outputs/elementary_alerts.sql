{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id'
  )
}}


with tables_changes as (

    select * from {{ ref('tables_changes_description') }}

),

columns_changes as (

    select * from {{ ref('columns_changes_description') }}

),

alerts_tables_changes as (

    select
        change_id as alert_id,
        detected_at,
        {{ full_table_name_to_schema() }} as schema_name,
        full_table_name,
        'schema_change' as alert_type,
        change as sub_type,
        change_description as alert_reason_value,
        array_construct('change_info') as alert_details_keys,
        array_construct(change_info) as alert_details_values
    from tables_changes
    where (full_table_name in {{ get_tables_for_alerts() }}
        or schema_name in {{ get_schemas_for_alerts() }})
        and full_table_name not in {{ get_excluded_tables_for_alerts() }}
),

alerts_columns_changes as (

    select
        change_id as alert_id,
        detected_at,
        {{ full_table_name_to_schema() }} as schema_name,
        full_table_name,
        'schema_change' as alert_type,
        change as sub_type,
        change_description as alert_reason_value,
        array_construct('change_info') as alert_details_keys,
        array_construct(change_info) as alert_details_values
    from columns_changes
    where (full_column_name in {{ get_columns_for_alerts() }}
        or full_table_name in {{ get_tables_for_alerts() }})
        and full_column_name not in {{ get_excluded_columns_for_alerts() }}

),

union_alerts as (

    select * from alerts_tables_changes
        union all
    select * from alerts_columns_changes
)

select * from union_alerts
