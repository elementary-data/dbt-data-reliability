{% macro get_data_type_synonyms(exact_data_type) %}
    {% set result = adapter.dispatch('get_data_type_synonyms','elementary')(exact_data_type) %}
    {{ return(result) }}
{% endmacro %}

{% macro default__get_data_type_synonyms(exact_data_type) %}
   {{return (exact_data_type) }}
{% endmacro %}

{% macro bigquery__get_data_type_synonyms(exact_data_type) %}
{# BigQuery has no concept of data type synonyms,
 see https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types #}
   {{return (exact_data_type) }}
{% endmacro %}



{% macro snowflake__get_data_type_synonyms(exact_data_type) %}
{# understanding Snowflake data type synonyms:
 https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html #}
 {% set exact_data_type_to_data_type_returned_by_the_info_schema = {'VARCHAR': 'TEXT',
                'STRING': 'TEXT',
                'NVARCHAR': 'TEXT',
                'NUMERIC': 'NUMBER',
                'DECIMAL': 'NUMBER',
                'INT':'NUMBER',
                'INTEGER':'NUMBER',
                'SMALLINT':'NUMBER',
                'BIGINT':'NUMBER',
                'TINYINT':'NUMBER',
                'BYTEINT':'NUMBER',
                'REAL': 'FLOAT',
                'DOUBLE':'FLOAT',
                'DOUBLE PRECISION': 'FLOAT'
                }%}
 {%- if exact_data_type in exact_data_type_to_data_type_returned_by_the_info_schema%}
   {{ return (exact_data_type_to_data_type_returned_by_the_info_schema[exact_data_type])}}
 {%- else %}
   {{return (exact_data_type) }}
 {%- endif%}
{% endmacro %}



{% macro spark__get_data_type_synonyms(exact_data_type) %}
{# spark also has no concept of data type synonyms :
   see https://spark.apache.org/docs/latest/sql-ref-datatypes.html #}
   {{return (exact_data_type) }}
{% endmacro %}
