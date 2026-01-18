{#
    Parse SLA time string and normalize to hour and minute components.
    
    Supports formats:
    - "7:00" or "07:00" -> {"hour": 7, "minute": 0}
    - "7am" or "7 AM" or "7:00am" -> {"hour": 7, "minute": 0}
    - "2pm" or "2 PM" or "2:30pm" -> {"hour": 14, "minute": 0} or {"hour": 14, "minute": 30}
    - "14:00" or "14:30" -> {"hour": 14, "minute": 0} or {"hour": 14, "minute": 30}
    
    Returns a dict with 'hour' (0-23) and 'minute' (0-59) keys.
    Raises an error if the time format is invalid.
#}

{% macro parse_sla_time(sla_time) %}
    {% set time_str = sla_time | string | trim | lower %}
    {% set re = modules.re %}
    
    {# Check for AM/PM format #}
    {% set am_pm_pattern = re.compile(r'^(\d{1,2})(?::(\d{2}))?\s*(am|pm)$', re.IGNORECASE) %}
    {% set am_pm_match = am_pm_pattern.match(time_str) %}
    
    {% if am_pm_match %}
        {% set hour = am_pm_match.group(1) | int %}
        {% set minute = (am_pm_match.group(2) or '0') | int %}
        {% set period = am_pm_match.group(3) | lower %}
        
        {# Validate hour for 12-hour format #}
        {% if hour < 1 or hour > 12 %}
            {{ exceptions.raise_compiler_error("Invalid hour '" ~ hour ~ "' in time '" ~ sla_time ~ "'. For AM/PM format, hour must be 1-12.") }}
        {% endif %}
        
        {# Convert to 24-hour format #}
        {% if period == 'am' %}
            {% if hour == 12 %}
                {% set hour = 0 %}
            {% endif %}
        {% else %}  {# pm #}
            {% if hour != 12 %}
                {% set hour = hour + 12 %}
            {% endif %}
        {% endif %}
        
        {% do return({'hour': hour, 'minute': minute}) %}
    {% endif %}
    
    {# Check for 24-hour format (HH:MM or H:MM) #}
    {% set time_24_pattern = re.compile(r'^(\d{1,2}):(\d{2})$') %}
    {% set time_24_match = time_24_pattern.match(time_str) %}
    
    {% if time_24_match %}
        {% set hour = time_24_match.group(1) | int %}
        {% set minute = time_24_match.group(2) | int %}
        
        {# Validate hour and minute #}
        {% if hour < 0 or hour > 23 %}
            {{ exceptions.raise_compiler_error("Invalid hour '" ~ hour ~ "' in time '" ~ sla_time ~ "'. Hour must be 0-23 for 24-hour format.") }}
        {% endif %}
        {% if minute < 0 or minute > 59 %}
            {{ exceptions.raise_compiler_error("Invalid minute '" ~ minute ~ "' in time '" ~ sla_time ~ "'. Minute must be 0-59.") }}
        {% endif %}
        
        {% do return({'hour': hour, 'minute': minute}) %}
    {% endif %}
    
    {# Check for hour-only format (just a number, interpreted as 24-hour) #}
    {% set hour_only_pattern = re.compile(r'^(\d{1,2})$') %}
    {% set hour_only_match = hour_only_pattern.match(time_str) %}
    
    {% if hour_only_match %}
        {% set hour = hour_only_match.group(1) | int %}
        
        {% if hour < 0 or hour > 23 %}
            {{ exceptions.raise_compiler_error("Invalid hour '" ~ hour ~ "' in time '" ~ sla_time ~ "'. Hour must be 0-23.") }}
        {% endif %}
        
        {% do return({'hour': hour, 'minute': 0}) %}
    {% endif %}
    
    {# No valid format matched #}
    {{ exceptions.raise_compiler_error("Invalid time format '" ~ sla_time ~ "'. Supported formats: '7:00', '07:00', '7am', '7:30am', '2pm', '2:30pm', '14:00', '14:30'.") }}
{% endmacro %}


{#
    Format parsed time as a string for display (HH:MM format).
#}
{% macro format_sla_time(parsed_time) %}
    {% set hour_str = '%02d' | format(parsed_time.hour) %}
    {% set minute_str = '%02d' | format(parsed_time.minute) %}
    {% do return(hour_str ~ ':' ~ minute_str) %}
{% endmacro %}
