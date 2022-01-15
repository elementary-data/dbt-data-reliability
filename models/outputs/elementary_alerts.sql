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
        {{ full_name_to_db() }},
        {{ full_name_to_schema() }},
        {{ full_name_to_table() }},
        NULL as column_name,
        'schema_change' as alert_type,
        change as sub_type,
        change_description as alert_description,
        false as alert_sent
    from tables_changes
    where (full_table_name in {{ get_tables_for_alerts() }}
        or schema_name in {{ get_schemas_for_alerts() }})
        and full_table_name not in {{ get_excluded_tables_for_alerts() }}
),

alerts_columns_changes as (

    select
        change_id as alert_id,
        detected_at,
        {{ full_name_to_db() }},
        {{ full_name_to_schema() }},
        {{ full_name_to_table() }},
        column_name,
        'schema_change' as alert_type,
        change as sub_type,
        change_description as alert_description,
        false as alert_sent
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
{% if is_incremental() %}
    {% set row_count = get_row_count(this) %}
    {% if row_count > 0 %}
        where detected_at > (select max(detected_at) from {{ this }})
    {%- endif %}
{%- endif %}
