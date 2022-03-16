{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id'
  )
}}

with dbt_runs as (

    select * from {{ elementary.get_source_path('edr_dbt_artifacts', 'dbt_run_results') }}

),

alerts_model_runs as (

    {%- if elementary.get_config_var('alert_dbt_model_fail') %}
     select
        model_execution_id as alert_id,
        generated_at as detected_at,
        {{ elementary.null_string() }} as database_name,
        {{ elementary.null_string() }} as schema_name,
        name as table_name,
        {{ elementary.null_string() }} as column_name,
        'dbt_model_failed' as alert_type,
        status as sub_type,
        {{ elementary.dbt_model_run_result_description() }} as alert_description,
        {{ elementary.null_string() }} as owner,
        {{ elementary.null_string() }} as tags,
        {{ elementary.null_string() }} as alert_results_query,
        {{ elementary.null_string() }} as other
    from dbt_runs
    where resource_type = 'model'
        {%- if elementary.get_config_var('alert_dbt_model_skip') %}
        and status in ('error','skipped')
        {%- else %}
        and status = 'error'
        {%- endif %}
    {%- else %}
        {{ elementary.empty_alerts() }}
    {%- endif %}

)

select * from alerts_model_runs