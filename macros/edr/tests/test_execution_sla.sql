{#
    Test: execution_sla
    
    Verifies that a dbt model was executed successfully before a specified SLA deadline time.
    Checks dbt_run_results for the model's execution history.
    
    Use case: "Did the pipeline complete on time?"
    
    Parameters:
        sla_time (required): Deadline time. Supports formats like "07:00", "7am", "2:30pm", "14:30"
        timezone (required): IANA timezone name (e.g., "America/Los_Angeles", "Europe/London")
        day_of_week (optional): Day(s) to check. String or list: "Monday", ["Monday", "Wednesday"]
        day_of_month (optional): Day(s) of month to check. Integer or list: 1, [1, 15]
    
    Schedule behavior:
        - If neither day_of_week nor day_of_month is set: check every day (default)
        - If day_of_week is set: only check on those days
        - If day_of_month is set: only check on those days
        - If both are set: check if today matches EITHER filter (OR logic)
    
    Example usage:
        models:
          - name: my_model
            tests:
              - elementary.execution_sla:
                  sla_time: "07:00"
                  timezone: "America/Los_Angeles"
              
          - name: weekly_report
            tests:
              - elementary.execution_sla:
                  sla_time: "6am"
                  timezone: "Europe/Amsterdam"
                  day_of_week: ["Monday", "Wednesday"]
                  
          - name: monthly_close
            tests:
              - elementary.execution_sla:
                  sla_time: "09:00"
                  timezone: "Asia/Tokyo"
                  day_of_month: [1, 15]
    
    Test passes if:
        - Today is not a scheduled check day (based on day_of_week/day_of_month)
        - OR the model was executed successfully before the SLA deadline today
        - OR the SLA deadline for today hasn't passed yet
    
    Test fails if:
        - Today is a scheduled check day AND:
            - The model was not executed today
            - The model was executed but only after the SLA deadline
            - All executions today failed
#}

{% test execution_sla(model, sla_time, timezone, day_of_week=none, day_of_month=none) %}
    {{ config(tags=['elementary-tests']) }}
    
    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}
        
        {# Validate required parameters #}
        {% if not sla_time %}
            {{ exceptions.raise_compiler_error("The 'sla_time' parameter is required. Example: sla_time: '07:00'") }}
        {% endif %}
        
        {# Validate timezone #}
        {% do elementary.validate_timezone(timezone) %}
        
        {# Normalize and validate day filters #}
        {% set day_of_week_filter = elementary.normalize_day_of_week(day_of_week) %}
        {% set day_of_month_filter = elementary.normalize_day_of_month(day_of_month) %}
        
        {# Get model relation and validate #}
        {% set model_relation = elementary.get_model_relation_for_test(model, elementary.get_test_model()) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("Unsupported model: " ~ model ~ " (this might happen if you override 'ref' or 'source')") }}
        {% endif %}
        
        {# Get the models unique_id for querying run results #}
        {% set test_node = elementary.get_test_model() %}
        {% set parent_model_unique_ids = elementary.get_parent_model_unique_ids_from_test_node(test_node) %}
        {% if parent_model_unique_ids | length == 0 %}
            {{ exceptions.raise_compiler_error("Could not determine parent model for this test.") }}
        {% endif %}
        {% set model_unique_id = parent_model_unique_ids[0] %}
        
        {# Parse the SLA time #}
        {% set parsed_time = elementary.parse_sla_time(sla_time) %}
        {% set formatted_sla_time = elementary.format_sla_time(parsed_time) %}
        
        {# Calculate SLA deadline in UTC (also returns current day info) #}
        {% set sla_info = elementary.calculate_sla_deadline_utc(parsed_time.hour, parsed_time.minute, timezone) %}
        
        {# Check if today is a scheduled check day #}
        {% set should_check = elementary.should_check_sla_today(
            sla_info.day_of_week, 
            sla_info.day_of_month, 
            day_of_week_filter, 
            day_of_month_filter
        ) %}
        
        {# If today is not a scheduled check day, skip (pass) #}
        {% if not should_check %}
            {{ elementary.edr_log('Skipping execution_sla test for ' ~ model_relation.identifier ~ ' - not a scheduled check day (' ~ sla_info.day_of_week ~ ', day ' ~ sla_info.day_of_month ~ ')') }}
            {{ elementary.no_results_query() }}
        {% else %}
        
        {{ elementary.edr_log('Running execution_sla test for ' ~ model_relation.identifier ~ ' with SLA ' ~ formatted_sla_time ~ ' ' ~ timezone) }}
        
        {# Build the query #}
        {{ elementary.get_execution_sla_query(
            model_unique_id=model_unique_id,
            model_name=model_relation.identifier,
            sla_deadline_utc=sla_info.sla_deadline_utc,
            target_date=sla_info.target_date,
            target_date_start_utc=sla_info.target_date_start_utc,
            target_date_end_utc=sla_info.target_date_end_utc,
            deadline_passed=sla_info.deadline_passed,
            formatted_sla_time=formatted_sla_time,
            timezone=timezone
        ) }}
        
        {% endif %}
        
    {%- else %}
        {{ elementary.no_results_query() }}
    {%- endif %}
    
{% endtest %}


{#
    Build SQL query to check if model was executed before SLA deadline.
    
    Logic:
    - Query dbt_run_results for successful runs of this model today (using UTC datetime range)
    - If any successful run completed before the deadline: SLA met
    - If deadline hasn't passed yet: Don't fail (still time)
    - Otherwise: SLA missed
#}
{% macro get_execution_sla_query(model_unique_id, model_name, sla_deadline_utc, target_date, target_date_start_utc, target_date_end_utc, deadline_passed, formatted_sla_time, timezone) %}
    
    {# Get reference to dbt_run_results table #}
    {% set run_results_relation = elementary.get_elementary_relation('dbt_run_results') %}
    
    {% if not run_results_relation %}
        {{ exceptions.raise_compiler_error("Could not find 'dbt_run_results' table. Make sure Elementary models are set up.") }}
    {% endif %}
    
    with 
    
    sla_deadline as (
        select 
            {{ elementary.edr_cast_as_timestamp("'" ~ sla_deadline_utc ~ "'") }} as deadline_utc,
            {{ elementary.edr_cast_as_timestamp("'" ~ target_date_start_utc ~ "'") }} as target_date_start_utc,
            {{ elementary.edr_cast_as_timestamp("'" ~ target_date_end_utc ~ "'") }} as target_date_end_utc,
            '{{ target_date }}' as target_date
    ),
    
    {# Get all runs for this model today (using UTC datetime range for target timezone's day) #}
    todays_runs as (
        select
            rr.unique_id,
            rr.name as model_name,
            {{ elementary.edr_cast_as_timestamp('rr.execute_completed_at') }} as completed_at_utc,
            rr.status
        from {{ run_results_relation }} rr
        cross join sla_deadline sd
        where rr.unique_id = '{{ model_unique_id }}'
          and rr.resource_type = 'model'
          and {{ elementary.edr_cast_as_timestamp('rr.execute_completed_at') }} >= sd.target_date_start_utc
          and {{ elementary.edr_cast_as_timestamp('rr.execute_completed_at') }} <= sd.target_date_end_utc
    ),
    
    successful_runs as (
        select * from todays_runs where status = 'success'
    ),
    
    {# Find runs that completed before the deadline #}
    runs_before_deadline as (
        select sr.*
        from successful_runs sr
        cross join sla_deadline sd
        where sr.completed_at_utc <= sd.deadline_utc
    ),
    
    sla_result as (
        select
            sd.target_date,
            sd.deadline_utc as sla_deadline_utc,
            (select min(completed_at_utc) from runs_before_deadline) as first_valid_run_utc,
            (select min(completed_at_utc) from successful_runs) as first_successful_run_utc,
            (select count(*) from todays_runs) as total_runs_today,
            (select count(*) from successful_runs) as successful_runs_today,
            case
                when exists (select 1 from runs_before_deadline) then 'MET_SLA'
                when exists (select 1 from successful_runs) then 'MISSED_SLA'
                when exists (select 1 from todays_runs) then 'ALL_FAILED'
                else 'NOT_RUN'
            end as sla_status
        from sla_deadline sd
    ),
    
    final_result as (
        select
            '{{ model_name }}' as model_name,
            '{{ model_unique_id }}' as model_unique_id,
            target_date,
            '{{ formatted_sla_time }}' as sla_time,
            '{{ timezone }}' as timezone,
            cast(sla_deadline_utc as {{ elementary.edr_type_string() }}) as sla_deadline_utc,
            sla_status,
            cast(first_valid_run_utc as {{ elementary.edr_type_string() }}) as first_valid_run_utc,
            cast(first_successful_run_utc as {{ elementary.edr_type_string() }}) as first_successful_run_utc,
            total_runs_today,
            successful_runs_today,
            case
                when sla_status = 'MET_SLA' then false
                {# If deadline hasn't passed, don't fail yet #}
                {% if deadline_passed %}
                when not TRUE then false
                {% else %}
                when not FALSE then false
                {% endif %}
                else true
            end as is_failure,
            case
                when sla_status = 'NOT_RUN' then 
                    'Model "{{ model_name }}" was not executed today. Expected before {{ formatted_sla_time }} {{ timezone }}.'
                when sla_status = 'ALL_FAILED' then
                    'Model "{{ model_name }}" ran ' || cast(total_runs_today as {{ elementary.edr_type_string() }}) || 
                    ' time(s) today but all executions failed.'
                when sla_status = 'MISSED_SLA' then
                    'Model "{{ model_name }}" first succeeded at ' || 
                    cast(first_successful_run_utc as {{ elementary.edr_type_string() }}) || 
                    ' UTC, which is after the SLA deadline of {{ formatted_sla_time }} {{ timezone }}.'
                else
                    'Model "{{ model_name }}" met SLA - executed successfully before {{ formatted_sla_time }} {{ timezone }}.'
            end as result_description
        from sla_result
    )
    
    select
        model_name,
        model_unique_id,
        target_date,
        sla_time,
        timezone,
        sla_deadline_utc,
        sla_status,
        first_valid_run_utc,
        first_successful_run_utc,
        total_runs_today,
        successful_runs_today,
        result_description
    from final_result
    where is_failure = true

{% endmacro %}
