{% macro data_type_list(data_type) %}
    {% set result = adapter.dispatch('data_type_list','elementary')(data_type) %}
    {{ return(result) }}
{% endmacro %}

{% macro default__data_type_list(data_type) %}

    {% set string_list = ['character varying','varchar','character','char','text','nchar','bpchar','string'] | list %}
    {% set numeric_list = ['integer', 'bigint','smallint','decimal','numeric','real','double precision','enum','int2','int4','int','int8','float8','float'] | list %}
    {% set timestamp_list = ['date', 'timestamp','timestamptz','timestamp without time zone','timestamp with time zone'] | list %}
    {% set boolean_list = ["boolean"] | list %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- elif data_type == 'timestamp' %}
        {{ return(timestamp_list) }}
    {%- elif data_type == "boolean" %}
        {{ return(boolean_list) }}
    {%- else %}
        {{ return([]) }}
    {%- endif %}

{% endmacro %}

{% macro bigquery__data_type_list(data_type) %}

    {% set string_list = ['STRING'] | list %}
    {% set numeric_list = ['INT64','NUMERIC','BIGNUMERIC','FLOAT64','INTEGER'] | list %}
    {% set timestamp_list = ['DATE','DATETIME','TIMESTAMP'] | list %}
    {% set boolean_list = ["BOOL", "BOOLEAN"] | list %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- elif data_type == 'timestamp' %}
        {{ return(timestamp_list) }}
    {%- elif data_type == "boolean" %} 
        {{ return(boolean_list) }}
    {%- else %}
        {{ return([]) }}
    {%- endif %}

{% endmacro %}


{% macro snowflake__data_type_list(data_type) %}

    {% set string_list = ['VARCHAR','CHAR','CHARACTER','STRING','TEXT'] | list %}
    {% set numeric_list = ['NUMBER','DECIMAL','NUMERIC','INT','INTEGER','BIGINT','SMALLINT','TINYINT','BYTEINT','FLOAT','FLOAT4','FLOAT8','DOUBLE','DOUBLE PRECISION','REAL'] | list %}
    {% set timestamp_list = ['DATE','DATETIME','TIME','TIMESTAMP','TIMESTAMP_LTZ','TIMESTAMP_NTZ','TIMESTAMP_TZ'] | list %}
    {% set boolean_list = ["BOOLEAN"] | list %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- elif data_type == 'timestamp' %}
        {{ return(timestamp_list) }}
    {%- elif data_type == "boolean" %}
        {{ return(boolean_list) }}
    {%- else %}
        {{ return([]) }}
    {%- endif %}

{% endmacro %}


{% macro spark__data_type_list(data_type) %}

    {% set string_list = ['string'] | list %}
    {% set numeric_list = ['int','bigint','smallint','tinyint','float','double','long','short','decimal'] | list %}
    {% set timestamp_list = ['timestamp','date'] | list %}
    {% set boolean_list = ["boolean"] | list %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- elif data_type == 'timestamp' %}
        {{ return(timestamp_list) }}
    {%- elif data_type == "boolean" %}
        {{ return(boolean_list) }}
    {%- else %}
        {{ return([]) }}
    {%- endif %}
    
{% endmacro %}


{% macro athena__data_type_list(data_type) %}

    {% set string_list = ['string', 'varchar', 'char'] | list %}
    {% set numeric_list = ['int','integer','bigint','smallint','tinyint','float','real','double','decimal'] | list %}
    {% set timestamp_list = ['timestamp','date'] | list %}
    {% set boolean_list = ["boolean"] | list %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- elif data_type == 'timestamp' %}
        {{ return(timestamp_list) }}
    {%- elif data_type == "boolean" %}
        {{ return(boolean_list) }}
    {%- else %}
        {{ return([]) }}
    {%- endif %}

{% endmacro %}

{% macro trino__data_type_list(data_type) %}

    {% set string_list = ['string', 'varchar', 'char'] | list %}
    {% set numeric_list = ['int','integer','bigint','smallint','tinyint','float','real','double','decimal'] | list %}
    {% set timestamp_list = ['timestamp','date'] | list %}
    {% set boolean_list = ["boolean"] | list %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- elif data_type == 'timestamp' %}
        {{ return(timestamp_list) }}
    {%- elif data_type == "boolean" %}
        {{ return(boolean_list) }}
    {%- else %}
        {{ return([]) }}
    {%- endif %}
 
{% endmacro %}

{% macro clickhouse__data_type_list(data_type) %}
    {% set string_list = ['String', 'FixedString', 'LowCardinality(String)'] | list %}
    {% set numeric_list = ['Int8', 'Int16', 'Int32', 'Int64', 'UInt8', 'UInt16', 'UInt32', 'UInt64', 'Float32', 'Float64', 'Decimal', 'Decimal32', 'Decimal64', 'Decimal128'] | list %}
    {% set timestamp_list = ['DateTime', 'Date', 'Date32'] | list %}
    {% set boolean_list = ["UInt8","Bool"] | list %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- elif data_type == 'timestamp' %}
        {{ return(timestamp_list) }}
    {%- elif data_type == "boolean" %}
        {{ return(boolean_list) }}
    {%- else %}
        {{ return([]) }}
    {%- endif %}

{% endmacro %}

{% macro dremio__data_type_list(data_type) %}
    {% set string_list = ['varchar', 'character varying'] | list %}
    {% set numeric_list = ['int','integer','bigint','double','decimal','float','smallint','tinyint'] | list %}
    {% set timestamp_list = ['date','time','timestamp', 'time with time zone', 'timestamp with time zone'] | list %}
    {% set boolean_list = ['boolean', 'bit'] | list %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- elif data_type == 'timestamp' %}
        {{ return(timestamp_list) }}
    {%- elif data_type == "boolean" %}
        {{ return(boolean_list) }}
    {%- else %}
        {{ return([]) }}
    {%- endif %}

{% endmacro %}
