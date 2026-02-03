{#
    Test: data_freshness_sla
    
    Verifies that data in a model was updated before a specified SLA deadline time.
    Checks the max timestamp value of a specified column in the data itself.
    
    Use case: "Is the data fresh?" / "Was the data updated on time?"
    
    Parameters:
        timestamp_column (required): Column name containing timestamps to check for freshness
        sla_time (required): Deadline time. Supports formats like "07:00", "7am", "2:30pm", "14:30"
        timezone (required): IANA timezone name (e.g., "America/Los_Angeles", "Europe/London")
        day_of_week (optional): Day(s) to check. String or list: "Monday", ["Monday", "Wednesday"]
        day_of_month (optional): Day(s) of month to check. Integer or list: 1, [1, 15]
        where_expression (optional): Additional WHERE clause filter for the data query
    
    Schedule behavior:
        - If neither day_of_week nor day_of_month is set: check every day (default)
        - If day_of_week is set: only check on those days
        - If day_of_month is set: only check on those days
        - If both are set: check if today matches EITHER filter (OR logic)
    
    Example usage:
        models:
          - name: my_model
            tests:
              - elementary.data_freshness_sla:
                  timestamp_column: updated_at
                  sla_time: "07:00"
                  timezone: "America/Los_Angeles"
              
          - name: daily_events
            tests:
              - elementary.data_freshness_sla:
                  timestamp_column: event_timestamp
                  sla_time: "6am"
                  timezone: "Europe/Amsterdam"
                  where_expression: "event_type = 'completed'"
                  
          - name: weekly_report_data
            tests:
              - elementary.data_freshness_sla:
                  timestamp_column: report_date
                  sla_time: "09:00"
                  timezone: "Asia/Tokyo"
                  day_of_week: ["Monday"]
    
    Test passes if:
        - Today is not a scheduled check day (based on day_of_week/day_of_month)
        - OR the max timestamp in the data is from today (before or after deadline)
        - OR the SLA deadline for today hasn't passed yet
    
    Test fails if:
        - Today is a scheduled check day AND the deadline has passed AND:
            - No data exists in the table
            - The max timestamp is from a previous day (data not updated today)
#}

{% test data_freshness_sla(model, timestamp_column, sla_time, timezone, day_of_week=none, day_of_month=none, where_expression=none) %}
    {{ config(tags=['elementary-tests']) }}
    
    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}
        
        {# Validate required parameters #}
        {% if not timestamp_column %}
            {{ exceptions.raise_compiler_error("The 'timestamp_column' parameter is required. Example: timestamp_column: 'updated_at'") }}
        {% endif %}
        
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
        
        {# Validate timestamp column exists and is a timestamp type #}
        {% set timestamp_column_data_type = elementary.find_normalized_data_type_for_column(model_relation, timestamp_column) %}
        {% if not elementary.is_column_timestamp(model_relation, timestamp_column, timestamp_column_data_type) %}
            {{ exceptions.raise_compiler_error("Column '" ~ timestamp_column ~ "' is not a timestamp type. The timestamp_column must be a timestamp or datetime column.") }}
        {% endif %}
        
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
            {{ elementary.edr_log('Skipping data_freshness_sla test for ' ~ model_relation.identifier ~ ' - not a scheduled check day (' ~ sla_info.day_of_week ~ ', day ' ~ sla_info.day_of_month ~ ')') }}
            {{ elementary.no_results_query() }}
        {% else %}
        
        {{ elementary.edr_log('Running data_freshness_sla test for ' ~ model_relation.identifier ~ ' with SLA ' ~ formatted_sla_time ~ ' ' ~ timezone) }}
        
        {# Build the query #}
        {{ elementary.get_data_freshness_sla_query(
            model_relation=model_relation,
            timestamp_column=timestamp_column,
            sla_deadline_utc=sla_info.sla_deadline_utc,
            target_date=sla_info.target_date,
            target_date_start_utc=sla_info.target_date_start_utc,
            target_date_end_utc=sla_info.target_date_end_utc,
            deadline_passed=sla_info.deadline_passed,
            formatted_sla_time=formatted_sla_time,
            timezone=timezone,
            where_expression=where_expression
        ) }}
        
        {% endif %}
        
    {%- else %}
        {{ elementary.no_results_query() }}
    {%- endif %}
    
{% endtest %}


{#
    Build SQL query to check if data was updated before SLA deadline.
    
    Logic:
    - Query the model table to get MAX(timestamp_column)
    - Convert max timestamp to UTC for comparison
    - If max timestamp is from today (in target timezone): data is fresh, SLA met
    - If deadline hasn't passed yet: Don't fail (still time)
    - Otherwise: Data is stale, SLA missed
#}
{% macro get_data_freshness_sla_query(model_relation, timestamp_column, sla_deadline_utc, target_date, target_date_start_utc, target_date_end_utc, deadline_passed, formatted_sla_time, timezone, where_expression) %}
    
    with 
    
    sla_deadline as (
        select 
            {{ elementary.edr_cast_as_timestamp("'" ~ sla_deadline_utc ~ "'") }} as deadline_utc,
            {{ elementary.edr_cast_as_timestamp("'" ~ target_date_start_utc ~ "'") }} as target_date_start_utc,
            {{ elementary.edr_cast_as_timestamp("'" ~ target_date_end_utc ~ "'") }} as target_date_end_utc,
            '{{ target_date }}' as target_date
    ),
    
    {# Get the max timestamp from the data #}
    max_data_timestamp as (
        select
            max({{ elementary.edr_cast_as_timestamp(timestamp_column) }}) as max_timestamp_utc
        from {{ model_relation }}
        {% if where_expression %}
        where {{ where_expression }}
        {% endif %}
    ),
    
    {# Determine freshness status #}
    freshness_result as (
        select
            sd.target_date,
            sd.deadline_utc as sla_deadline_utc,
            mdt.max_timestamp_utc,
            case
                {# Data was updated today (max timestamp is within today's UTC range) #}
                when mdt.max_timestamp_utc >= sd.target_date_start_utc 
                     and mdt.max_timestamp_utc <= sd.target_date_end_utc then 'DATA_FRESH'
                {# No data exists #}
                when mdt.max_timestamp_utc is null then 'NO_DATA'
                {# Data exists but is from a previous day #}
                else 'DATA_STALE'
            end as freshness_status
        from sla_deadline sd
        cross join max_data_timestamp mdt
    ),
    
    final_result as (
        select
            '{{ model_relation.identifier }}' as model_name,
            target_date,
            '{{ formatted_sla_time }}' as sla_time,
            '{{ timezone }}' as timezone,
            cast(sla_deadline_utc as {{ elementary.edr_type_string() }}) as sla_deadline_utc,
            freshness_status,
            cast(max_timestamp_utc as {{ elementary.edr_type_string() }}) as max_timestamp_utc,
            case
                when freshness_status = 'DATA_FRESH' then false
                {# If deadline hasn't passed, don't fail yet #}
                {% if deadline_passed %}
                when not TRUE then false
                {% else %}
                when not FALSE then false
                {% endif %}
                else true
            end as is_failure,
            case
                when freshness_status = 'NO_DATA' then 
                    'No data found in "{{ model_relation.identifier }}"' ||
                    {% if where_expression %}
                    ' (with filter: {{ where_expression }})' ||
                    {% endif %}
                    '. Expected data to be updated before {{ formatted_sla_time }} {{ timezone }}.'
                when freshness_status = 'DATA_STALE' then
                    'Data in "{{ model_relation.identifier }}" is stale. Last update was at ' || 
                    cast(max_timestamp_utc as {{ elementary.edr_type_string() }}) || 
                    ' UTC, which is before today. Expected fresh data before {{ formatted_sla_time }} {{ timezone }}.'
                else
                    'Data in "{{ model_relation.identifier }}" is fresh - last update at ' || 
                    cast(max_timestamp_utc as {{ elementary.edr_type_string() }}) || 
                    ' UTC (before SLA deadline {{ formatted_sla_time }} {{ timezone }}).'
            end as result_description
        from freshness_result
    )
    
    select
        model_name,
        target_date,
        sla_time,
        timezone,
        sla_deadline_utc,
        freshness_status,
        max_timestamp_utc,
        result_description
    from final_result
    where is_failure = true

{% endmacro %}
