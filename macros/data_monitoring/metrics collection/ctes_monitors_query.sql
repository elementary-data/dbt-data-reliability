{% macro table_monitors_cte(table_monitors) %}

    {%- if table_monitors is defined and table_monitors|length %}
        {%- for table_monitor in table_monitors -%}
            {%- set monitor_macro = get_monitor_macro(table_monitor) %}
            select
                null as column_name,
                '{{ table_monitor }}' as metric_name,
                {{ monitor_macro() }} as metric_value
            from
                timeframe_data
                {% if not loop.last %} union all {%- endif %}
        {%- endfor -%}

    {%- else %}
        {{ empty_table([('column_name', 'string'), ('metric_name', 'string'), ('metric_value', 'int')]) }}
    {%- endif %}

{% endmacro %}


{% macro column_monitors_cte(column_config) %}

    {%- if column_config is defined and column_config|length %}
        {%- for monitored_column in column_config -%}
            {%- set monitored_column = column_config[loop.index0]['column_name'] %}
            {%- for column_monitor in column_config[loop.index0]['column_monitors'] %}
                {%- set monitor_macro = get_monitor_macro(column_monitor) %}
                select
                    '{{ monitored_column }}' as column_name,
                    '{{ column_monitor }}' as metric_name,
                    {{ monitor_macro(monitored_column) }} as metric_value
                from
                    timeframe_data
                    {% if not loop.last %} union all {%- endif %}
            {%- endfor %}
            {% if not loop.last %} union all {%- endif %}
        {%- endfor -%}

    {%- else %}
        {{ empty_table([('column_name', 'string'), ('metric_name', 'string'), ('metric_value', 'int')]) }}
    {%- endif %}

{% endmacro %}
