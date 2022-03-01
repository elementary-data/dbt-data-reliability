{% macro table_monitors_cte(table_monitors, timestamp_field, timeframe_end, full_table_name) %}

    {%- set executed_table_monitors = [] %}
    {%- if table_monitors %}
        {%- for table_monitor in table_monitors %}
            {%- set monitor_macro = elementary.get_monitor_macro(table_monitor) %}
            {%- if table_monitor == 'freshness' and timestamp_field %}
                {%- do executed_table_monitors.append(table_monitor) %}
                select
                    null as column_name,
                    '{{ table_monitor }}' as metric_name,
                    {{ monitor_macro(timestamp_field, timeframe_end) }} as metric_value
                from {{ full_table_name }}
                where {{ timestamp_field }} <= {{ timeframe_end }}
            {%- else %}
                {%- do executed_table_monitors.append(table_monitor) %}
                select
                    null as column_name,
                    '{{ table_monitor }}' as metric_name,
                    {{ monitor_macro() }} as metric_value
                from timeframe_data
            {%- endif %}
            {% if not loop.last %} union all {% endif %}
        {%- endfor -%}
    {%- endif %}

    {%- if not executed_table_monitors %}
        {{ elementary.empty_table([('column_name', 'string'), ('metric_name', 'string'), ('metric_value', 'int')]) }}
    {%- endif %}

{% endmacro %}


{% macro column_monitors_cte(column_config) %}

    {%- set executed_column_monitors = [] %}
    {%- if column_config %}
        {%- for monitored_column in column_config -%}
            {%- set monitored_column = column_config[loop.index0]['column_name'] %}
            {%- for column_monitor in column_config[loop.index0]['column_monitors'] %}
                {%- set monitor_macro = elementary.get_monitor_macro(column_monitor) %}
                {%- set column_name = elementary.column_quote(monitored_column) %}
                {%- if monitor_macro and column_name %}
                    {%- do executed_column_monitors.append(column_monitor) %}
                    select
                        '{{ monitored_column }}' as column_name,
                        '{{ column_monitor }}' as metric_name,
                        {{ monitor_macro(column_name) }} as metric_value
                    from timeframe_data
                        {%- if column_monitor in var('edr_monitors')['column_numeric'] %}
                            where {{ column_name }} < {{ var('max_int') }}
                        {%- endif %}
                        {% if not loop.last %} union all {% endif %}
                {%- endif %}
            {%- endfor %}
            {% if not loop.last %} union all {% endif %}
        {%- endfor -%}
    {%- endif %}

    {%- if not executed_column_monitors %}
        {{ elementary.empty_table([('column_name', 'string'), ('metric_name', 'string'), ('metric_value', 'int')]) }}
    {%- endif %}

{% endmacro %}
