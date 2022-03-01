{% macro data_type_list(data_type) %}
    {% set result = adapter.dispatch('data_type_list')(data_type) %}
    {{ return(result) }}
{% endmacro %}

{% macro default__data_type_list(data_type) %}

    {% set string_list = ['character varying','varchar','character','char','text'] | list %}

    {% set numeric_list = ['integer', 'bigint','smallint','decimal','numeric','real','double precision','enum'] | list %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- else%}
        {{ return([]) }}
    {%- endif %}

    {% endmacro %}

{% macro bigquery__data_type_list(data_type) %}

    {% set string_list = ['STRING'] | list %}

    {% set numeric_list = ['INT64','NUMERIC','BIGNUMERIC','FLOAT64','INTEGER'] | list %}

    {%- if data_type == 'string' %}
        {{ return(string_list) }}
    {%- elif data_type == 'numeric' %}
        {{ return(numeric_list) }}
    {%- else%}
        {{ return([]) }}
    {%- endif %}

{% endmacro %}


{% macro snowflake__data_type_list(data_type) %}

    {% set string_list = ['VARCHAR','CHAR','CHARACTER','STRING','TEXT'] | list %}

    {% set numeric_list = ['NUMBER','DECIMAL','NUMERIC','INT','INTEGER','BIGINT','SMALLINT','TINYINT','BYTEINT','FLOAT','FLOAT4','FLOAT8','DOUBLE','DOUBLE PRECISION','REAL'] | list %}

    {% set datetime_list = ['DATE','DATETIME','TIME','TIMESTAMP','TIMESTAMP_LTZ','TIMESTAMP_NTZ','TIMESTAMP_TZ'] | list %}

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