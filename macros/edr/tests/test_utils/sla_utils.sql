{#
    Shared utilities for SLA tests.
#}


{#
    Validate that a timezone string is a valid IANA timezone name.
    Raises a clear error if invalid.
#}
{% macro validate_timezone(timezone) %}
    {% set pytz = modules.pytz %}
    
    {% if not timezone %}
        {{ exceptions.raise_compiler_error("The 'timezone' parameter is required. Example: timezone: 'America/Los_Angeles'") }}
    {% endif %}
    
    {# Check if timezone is in pytz's list of all timezones #}
    {% if timezone not in pytz.all_timezones %}
        {{ exceptions.raise_compiler_error(
            "Invalid timezone '" ~ timezone ~ "'. Must be a valid IANA timezone name.\n" ~
            "Common examples:\n" ~
            "  - America/Los_Angeles (US Pacific)\n" ~
            "  - America/New_York (US Eastern)\n" ~
            "  - Europe/London (UK)\n" ~
            "  - Europe/Amsterdam (Netherlands)\n" ~
            "  - Asia/Tokyo (Japan)\n" ~
            "See: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones"
        ) }}
    {% endif %}
{% endmacro %}


{#
    Calculate the SLA deadline in UTC for today in the given timezone.
    Uses Python at compile time - no database-specific timezone functions needed.
    
    Returns: {
        'sla_deadline_utc': string (YYYY-MM-DD HH:MM:SS),
        'target_date': string (YYYY-MM-DD),
        'target_date_start_utc': string (YYYY-MM-DD HH:MM:SS) - start of day in UTC,
        'target_date_end_utc': string (YYYY-MM-DD HH:MM:SS) - end of day in UTC,
        'deadline_passed': boolean
    }
#}
{% macro calculate_sla_deadline_utc(sla_hour, sla_minute, timezone) %}
    {% set datetime = modules.datetime %}
    {% set pytz = modules.pytz %}
    
    {# Get timezone objects #}
    {% set utc_tz = pytz.timezone('UTC') %}
    {% set target_tz = pytz.timezone(timezone) %}
    
    {# Get current time in UTC and target timezone #}
    {% set now_utc = datetime.datetime.now(utc_tz) %}
    {% set now_local = now_utc.astimezone(target_tz) %}
    
    {# Target date is today in the target timezone #}
    {% set target_date_local = now_local.date() %}
    
    {# Create start of day (00:00:00) in target timezone #}
    {% set day_start_naive = datetime.datetime.combine(target_date_local, datetime.time(0, 0, 0)) %}
    {% set day_start_local = target_tz.localize(day_start_naive, is_dst=False) %}
    {% set day_start_utc = day_start_local.astimezone(utc_tz) %}
    
    {# Create end of day (23:59:59.999) in target timezone #}
    {% set day_end_naive = datetime.datetime.combine(target_date_local, datetime.time(23, 59, 59)) %}
    {% set day_end_local = target_tz.localize(day_end_naive, is_dst=False) %}
    {% set day_end_utc = day_end_local.astimezone(utc_tz) %}
    
    {# Create the SLA deadline in target timezone #}
    {# Use is_dst=False to resolve ambiguous times during DST transitions to standard time #}
    {% set sla_time_local = datetime.time(sla_hour, sla_minute, 0) %}
    {% set sla_deadline_naive = datetime.datetime.combine(target_date_local, sla_time_local) %}
    {% set sla_deadline_local = target_tz.localize(sla_deadline_naive, is_dst=False) %}
    
    {# Convert to UTC #}
    {% set sla_deadline_utc = sla_deadline_local.astimezone(utc_tz) %}
    
    {# Check if deadline has passed #}
    {% set deadline_passed = now_utc > sla_deadline_utc %}
    
    {# Format for SQL #}
    {% set sla_deadline_utc_str = sla_deadline_utc.strftime('%Y-%m-%d %H:%M:%S') %}
    {% set target_date_str = target_date_local.strftime('%Y-%m-%d') %}
    {% set day_start_utc_str = day_start_utc.strftime('%Y-%m-%d %H:%M:%S') %}
    {% set day_end_utc_str = day_end_utc.strftime('%Y-%m-%d %H:%M:%S') %}
    
    {{ return({
        'sla_deadline_utc': sla_deadline_utc_str,
        'target_date': target_date_str,
        'target_date_start_utc': day_start_utc_str,
        'target_date_end_utc': day_end_utc_str,
        'deadline_passed': deadline_passed,
        'day_of_week': now_local.strftime('%A'),
        'day_of_month': now_local.day
    }) }}
{% endmacro %}


{#
    Normalize day_of_week parameter to a list of lowercase day names.
    Accepts: single string, list of strings, or none.
    Returns: list of lowercase day names, or empty list if none provided.
#}
{% macro normalize_day_of_week(day_of_week) %}
    {% set valid_days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'] %}
    
    {% if day_of_week is none or day_of_week == '' %}
        {{ return([]) }}
    {% endif %}
    
    {# Convert single value to list #}
    {% if day_of_week is string %}
        {% set days = [day_of_week] %}
    {% else %}
        {% set days = day_of_week %}
    {% endif %}
    
    {# Validate and normalize each day #}
    {% set normalized = [] %}
    {% for day in days %}
        {% set day_lower = day | string | trim | lower %}
        {% if day_lower not in valid_days %}
            {{ exceptions.raise_compiler_error(
                "Invalid day_of_week '" ~ day ~ "'. Must be one of: Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday."
            ) }}
        {% endif %}
        {% do normalized.append(day_lower) %}
    {% endfor %}
    
    {{ return(normalized) }}
{% endmacro %}


{#
    Normalize day_of_month parameter to a list of integers.
    Accepts: single integer, list of integers, or none.
    Returns: list of integers (1-31), or empty list if none provided.
#}
{% macro normalize_day_of_month(day_of_month) %}
    {% if day_of_month is none or day_of_month == '' %}
        {{ return([]) }}
    {% endif %}
    
    {# Convert single value to list #}
    {% if day_of_month is number %}
        {% set days = [day_of_month] %}
    {% elif day_of_month is string %}
        {% set days = [day_of_month | int] %}
    {% else %}
        {% set days = day_of_month %}
    {% endif %}
    
    {# Validate each day #}
    {% set normalized = [] %}
    {% for day in days %}
        {% set day_int = day | int %}
        {% if day_int < 1 or day_int > 31 %}
            {{ exceptions.raise_compiler_error(
                "Invalid day_of_month '" ~ day ~ "'. Must be between 1 and 31."
            ) }}
        {% endif %}
        {% do normalized.append(day_int) %}
    {% endfor %}
    
    {{ return(normalized) }}
{% endmacro %}


{#
    Check if the SLA test should run today based on day_of_week and day_of_month filters.
    
    Logic (OR):
    - If neither filter is set: run every day (return true)
    - If only day_of_week is set: run if today matches any day in list
    - If only day_of_month is set: run if today matches any day in list
    - If both are set: run if today matches EITHER filter (OR logic)
    
    Parameters:
        current_day_of_week: string (e.g., "Monday")
        current_day_of_month: integer (e.g., 15)
        day_of_week_filter: list of lowercase day names (e.g., ["monday", "wednesday"])
        day_of_month_filter: list of integers (e.g., [1, 15])
    
    Returns: boolean
#}
{% macro should_check_sla_today(current_day_of_week, current_day_of_month, day_of_week_filter, day_of_month_filter) %}
    {# If no filters are set, run every day #}
    {% if day_of_week_filter | length == 0 and day_of_month_filter | length == 0 %}
        {{ return(true) }}
    {% endif %}
    
    {# Check day of week filter #}
    {% set current_dow_lower = current_day_of_week | lower %}
    {% if current_dow_lower in day_of_week_filter %}
        {{ return(true) }}
    {% endif %}
    
    {# Check day of month filter #}
    {% if current_day_of_month in day_of_month_filter %}
        {{ return(true) }}
    {% endif %}
    
    {# No filter matched #}
    {{ return(false) }}
{% endmacro %}
