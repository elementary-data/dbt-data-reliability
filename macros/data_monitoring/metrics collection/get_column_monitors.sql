{% macro get_column_monitors(data_type, config_monitors) %}

    {%- set all_types_monitors_except_schema %}
        [
        'null_percent'
        ]
    {% endset %}

    {%- set numeric_monitors %}
        [
        'min',
        'max'
        ]
    {% endset %}

    {%- set string_monitors %}
        [
        'min_length',
        'max_length'
        ]
    {% endset %}

    {%- set column_monitors = [] %}

    {% set all_types_intersect = lists_intersection(config_monitors, all_types_monitors_except_schema) %}
    {% for monitor in all_types_intersect %}
        {{ column_monitors.append(monitor) }}
    {% endfor %}

    {% if data_type == 'numeric' %}
        {% set numeric_intersect = lists_intersection(config_monitors, numeric_monitors) %}
        {% for monitor in numeric_intersect %}
            {{ column_monitors.append(monitor) }}
        {% endfor %}
    {% endif %}

    {% if data_type == 'string' %}
        {% set string_intersect = lists_intersection(config_monitors, string_monitors) %}
        {% for monitor in string_intersect %}
            {{ column_monitors.append(monitor) }}
        {% endfor %}
    {% endif %}

    {{ return(column_monitors) }}

{% endmacro %}


{% macro convert_data_type(data_type) %}
    {% set result = adapter.dispatch('convert_data_type')(data_type) %}
    {{ return(result) }}
{% endmacro %}

{% macro default__convert_data_type(data_type) %}

    {% if data_type in [
        'character varying',
        'varchar',
        'character',
        'char',
        'text'
    ] %}
        {{ return('string') }}

    {% elif data_type in [
        'integer',
        'bigint',
        'smallint',
        'decimal',
        'numeric',
        'real',
        'double precision',
        'enum',
        ] %}
        {{ return('numeric') }}

    {% else %}
        {{ return('other') }}
    {% endif %}

{% endmacro %}

{% macro bigquery__convert_data_type(data_type) %}

    {% if data_type in [
        'STRING'
    ] %}
        {{ return('string') }}

    {% elif data_type in [
        "INT64",
        "NUMERIC",
        "BIGNUMERIC",
        "FLOAT64",
        "INTEGER"
        ]
    %}
        {{ return('numeric') }}

    {% else %}
        {{ return('other') }}
    {% endif %}

{% endmacro %}

{% macro snowflake__convert_data_type(data_type) %}

    {% if data_type in [
        'VARCHAR',
        'CHAR',
        'CHARACTER',
        'STRING',
        'TEXT'
    ] %}
        {{ return('string') }}

    {% elif data_type in [
        'NUMBER',
        'DECIMAL',
        'NUMERIC',
        'INT',
        'INTEGER',
        'BIGINT',
        'SMALLINT',
        'TINYINT',
        'BYTEINT',
        'FLOAT',
        'FLOAT4',
        'FLOAT8',
        'DOUBLE',
        'DOUBLE PRECISION',
        'REAL',
    ] %}
        {{ return('numeric') }}

    {% else %}
        {{ return('other') }}
    {% endif %}

{% endmacro %}


