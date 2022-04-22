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
        {{ elementary.cast_as_string('change_id') }} as alert_id,
        {{ elementary.cast_as_timestamp('detected_at') }} as detected_at,
        {{ elementary.cast_as_string('database_name') }} as database_name,
        {{ elementary.cast_as_string('schema_name') }} as schema_name,
        {{ elementary.cast_as_string('table_name') }} as table_name,
        {{ elementary.null_string() }} as column_name,
        {{ elementary.cast_as_string("'schema_change'") }} as alert_type,
        {{ elementary.cast_as_string('change') }} as sub_type,
        {{ elementary.cast_as_string('change_description') }} as alert_description,
        {{ elementary.null_string() }} as owner,
        {{ elementary.null_string() }} as tags,
        {{ elementary.null_string() }} as alert_results_query,
        {{ elementary.null_string() }} as other
    from table_changes
    where ({{ elementary.full_schema_name() }} in {{ elementary.schemas_to_alert_on_new_tables() }} and change = 'table_added')

)

select * from table_changes_alerts_filtered

{% if is_incremental() %}
    {% set row_count = elementary.get_row_count(this) %}
    {% if row_count > 0 %}
        where detected_at > (select max(detected_at) from {{ this }})
    {%- endif %}
{%- endif %}
