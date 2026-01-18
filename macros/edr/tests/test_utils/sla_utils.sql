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
    
    {# Create the SLA deadline in target timezone #}
    {% set sla_time_local = datetime.time(sla_hour, sla_minute, 0) %}
    {% set sla_deadline_naive = datetime.datetime.combine(target_date_local, sla_time_local) %}
    {% set sla_deadline_local = target_tz.localize(sla_deadline_naive) %}
    
    {# Convert to UTC #}
    {% set sla_deadline_utc = sla_deadline_local.astimezone(utc_tz) %}
    
    {# Check if deadline has passed #}
    {% set deadline_passed = now_utc > sla_deadline_utc %}
    
    {# Format for SQL #}
    {% set sla_deadline_utc_str = sla_deadline_utc.strftime('%Y-%m-%d %H:%M:%S') %}
    {% set target_date_str = target_date_local.strftime('%Y-%m-%d') %}
    
    {{ return({
        'sla_deadline_utc': sla_deadline_utc_str,
        'target_date': target_date_str,
        'deadline_passed': deadline_passed
    }) }}
{% endmacro %}
