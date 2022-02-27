{% macro data_type_list(data_type) %}
    {{ adapter.dispatch('data_type_list')(data_type) }}
{% endmacro %}

{% macro default__data_type_list(data_type) %}

    {% set string_list = [
        'character varying',
        'varchar',
        'character',
        'char',
        'text'
    ] %}

    {% set numeric_list = [
        'integer',
        'bigint',
        'smallint',
        'decimal',
        'numeric',
        'real',
        'double precision',
        'enum',
        ] %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- else%}
        {{ return([]) }}
    {%- endif %}

    {% endmacro %}

{% macro bigquery__data_type_list(data_type) %}

    {% set string_list = [
        'STRING'
    ] %}

    {% set numeric_list = [
        "INT64",
        "NUMERIC",
        "BIGNUMERIC",
        "FLOAT64",
        "INTEGER"
        ]
    %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- else%}
        {{ return([]) }}
    {%- endif %}

{% endmacro %}


{% macro snowflake__data_type_list(data_type) %}

    {% set string_list = [
        'VARCHAR',
        'CHAR',
        'CHARACTER',
        'STRING',
        'TEXT'
   ] %}

    {% set numeric_list = [
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

    {% set datetime_list = [
        'DATE',
        'DATETIME',
        'TIME',
        'TIMESTAMP',
        'TIMESTAMP_LTZ',
        'TIMESTAMP_NTZ',
        'TIMESTAMP_TZ'
    ] %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- elif data_type == 'datetime' %}
        {{ return(datetime_list) }}
    {%- else %}
        {{ return([]) }}
    {%- endif %}

{% endmacro %}