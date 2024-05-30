{% macro edr_log(msg, info=True) %}
    {%- if execute %}
        {% do log('Elementary: ' ~ msg, info=info) %}
    {%- endif %}
{% endmacro %}

{% macro edr_log_warning(msg, info=True) %}
    {%- if execute %}
        {% do elementary.edr_log("Warning - " ~ msg, info=info) %}
    {%- endif %}
{% endmacro %}

{% macro file_log(msg) %}
    {% if execute %}
        {% do elementary.edr_log(msg, info=false) %}
        {% do elementary.debug_log(msg) %}
    {% endif %}
{% endmacro %}

{% macro debug_log(msg) %}
    {%- if execute %}
        {% set debug_logs_enabled = elementary.get_config_var('debug_logs') %}
        {% if debug_logs_enabled %}
            {{ elementary.edr_log(msg) }}
        {% endif %}
    {%- endif %}
{% endmacro %}


{% macro test_log(msg_type, table_name, column_name=none) %}
    {%- if column_name %}
        {%- set start = 'Started running data monitors on: ' ~ table_name ~ ' ' ~ column_name %}
        {%- set end = 'Finished running data monitors on: ' ~ table_name ~ ' ' ~ column_name %}
    {%- else %}
        {%- set start = 'Started running data monitors on: ' ~ table_name %}
        {%- set end = 'Finished running data monitors on: ' ~ table_name %}
    {%- endif %}

    {%- if msg_type == 'start' %}
        {% do elementary.edr_log(start) %}
    {%- elif msg_type == 'end' %}
        {% do elementary.edr_log(end) %}
    {%- endif %}
{% endmacro %}

{% macro begin_duration_measure_context(context_name) %}
    {% set duration_context_stack = elementary.get_duration_context_stack() %}
    {% if duration_context_stack is none %}
        {# If the duration stack is not initialized, it means we're not called from the package #}
        {% do return(none) %}
    {% endif %}

    {% do duration_context_stack.append(elementary.init_duration_context_dict(context_name)) %}
{% endmacro %}

{% macro end_duration_measure_context(context_name, log_durations=false) %}
    {% set duration_context_stack = elementary.get_duration_context_stack() %}
    {% if duration_context_stack is none %}
        {# If the duration stack is not initialized, it means we're not called from the package #}
        {% do return(none) %}
    {% endif %}

    {% set context_index = elementary.get_duration_context_index(context_name) %}
    {% if context_index is none %}
        {% do elementary.debug_log('warning - end_duration_measure_context called without matching start_duration_measure_context') %}
        {% do return(none) %}
    {% endif %}

    {% set cur_context = namespace(data=none) %}
    {% for _ in range(context_index) %}
        {% set cur_context.data = elementary.pop_duration_context() %}
    {% endfor %}

    {% if log_durations %}
        {% do elementary.file_log('Measured durations for context - ' ~ context_name ~ ':') %}
        {% for sub_context_name, sub_context_duration in cur_context.data.durations.items() %}
            {% set num_runs = cur_context.data.num_runs.get(sub_context_name, 'N/A') %}
            {% do elementary.file_log('    ' ~ sub_context_name ~ ': ' ~ sub_context_duration ~ ' (' ~ num_runs ~ ' runs)') %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro init_duration_context_dict(context_name) %}
    {% do return({
        "name": context_name,
        "start_time": modules.datetime.datetime.utcnow(),
        "durations": {},
        "num_runs": {}
    }) %}
{% endmacro %}

{% macro get_duration_context_stack() %}
  {% set global_duration_context_stack = elementary.get_cache('duration_context_stack') %}
  {% if global_duration_context_stack is none %}
        {# If the duration stack is not initialized, it means we're not called from the package #}
        {% do return(none) %}
    {% endif %}

  {% set thread_stack = global_duration_context_stack.get(thread_id) %}
  {% if not thread_stack %}
    {% do global_duration_context_stack.update({thread_id: [elementary.init_duration_context_dict('main')]}) %}
  {% endif %}
  {{ return(global_duration_context_stack.get(thread_id)) }}
{% endmacro %}

{% macro get_duration_context_index(context_name) %}
    {% set duration_context_stack = elementary.get_duration_context_stack() %}
    {% for context in duration_context_stack | reverse %}
        {% if context.name == context_name %}
            {% do return(loop.index) %}
        {% endif %}
    {% endfor %}
    {% do return(none) %}
{% endmacro %}

{% macro pop_duration_context() %}
    {% set duration_context_stack = elementary.get_duration_context_stack() %}

    {# Pop current context and calculate total duration for it #}
    {% set cur_context = duration_context_stack.pop() %}
    {% do cur_context.durations.update({
        cur_context.name: modules.datetime.datetime.utcnow() - cur_context.start_time
    }) %}
    {% do cur_context.num_runs.update({
        cur_context.name: 1
    }) %}

    {# Merge durations and num runs to parent context #}
    {% if duration_context_stack | length > 0 %}
        {% set parent_context = duration_context_stack[-1] %}
        {% for sub_context_name, sub_context_duration in cur_context.durations.items() %}
            {% set full_sub_context_name = parent_context.name ~ '.' ~ sub_context_name %}
            {% set existing_duration = parent_context.durations.get(full_sub_context_name, modules.datetime.timedelta()) %}

            {% do parent_context.durations.update({
                full_sub_context_name: existing_duration + sub_context_duration,
            }) %}
        {% endfor %}
        {% for sub_context_name, sub_context_num_runs in cur_context.num_runs.items() %}
            {% set full_sub_context_name = parent_context.name ~ '.' ~ sub_context_name %}
            {% set existing_num_runs = parent_context.num_runs.get(full_sub_context_name, 0) %}

            {% do parent_context.num_runs.update({
                full_sub_context_name: existing_num_runs + sub_context_num_runs
            }) %}
        {% endfor %}
    {% endif %}

    {% do return(cur_context) %}
{% endmacro %}

{% macro get_stack_contexts() %}
    {% set duration_context_stack = elementary.get_duration_context_stack() %}
    {% set names = []%}
    {% for context in duration_context_stack %}
        {% do names.append(context.name) %}
    {% endfor %}
    {% do return(names) %}
{% endmacro %}
