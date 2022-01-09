-- TODO: array_construct is snowflake only

with json_validation_alerts as (
    select
        elementary_run_at as detected_at,
        full_table_name,
        'jason_validation' as alert_type,
        'bad_jsons_rate' as alert_reason,
        cast(bad_jsons_rate as {{ dbt_utils.type_string() }}) as alert_reason_value,
        array_construct('bad_jsons', 'total_jsons', 'min_timestamp', 'max_timestamp', 'validation_details') as alert_details_keys,
        array_construct(bad_jsons, total_jsons, min_timestamp, max_timestamp, validation_details) as alert_details_values
    from {{ ref('json_schema_validation_results') }}
    where
        elementary_run_at = (select max(elementary_run_at) from {{ ref('json_schema_validation_results') }})
        and bad_jsons_rate > {{ var('elementary')['bad_jsons_rate_threshold'] }}
),
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
    select * from json_validation_alerts
    union all
    select * from alertable_schema_changes
)
select * from final
