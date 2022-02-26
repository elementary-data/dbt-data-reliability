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
    {   { return('other') }}
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