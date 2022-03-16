{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id'
  )
}}


-- depends_on: {{ ref('column_changes') }}

{# these are just schema level alerts #}
{# table level alerts arrive from tests #}

with table_changes as (

    select * from {{ ref('table_changes') }}

),

table_changes_alerts_filtered as (

    select
        change_id as alert_id,
        detected_at,
        database_name,
        schema_name,
        table_name,
        {{ elementary.null_string() }} as column_name,
        'schema_change' as alert_type,
        change as sub_type,
        change_description as alert_description,
        {{ elementary.null_string() }} as owner,
        {{ elementary.null_string() }} as tags,
        {{ elementary.null_string() }} as alert_results_query,
        {{ elementary.null_string() }} as other
    from table_changes
    where ({{ elementary.full_schema_name() }} in {{ elementary.schemas_to_alert_on_new_tables() }} and sub_type = 'table_added')

)

select * from table_changes_alerts_filtered

{% if is_incremental() %}
    {% set row_count = elementary.get_row_count(this) %}
    {% if row_count > 0 %}
        where detected_at > (select max(detected_at) from {{ this }})
    {%- endif %}
{%- endif %}
