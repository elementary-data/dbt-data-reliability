-- TODO: array_construct is snowflake only

with
alertable_schema_changes as (
    select
        detected_at,
        full_table_name,
        'schema_change_from_configuration' as alert_type,
        change as alert_reason,
        description as alert_reason_value,
        array_construct('info') as alert_details_keys,
        array_construct(info) as alert_details_values
    from {{ ref('alertable_schema_changes_description') }}
    where
        detected_at = (select max(detected_at) from {{ ref('alertable_schema_changes_description') }})

),
final as (
    select * from alertable_schema_changes
)
select * from final
