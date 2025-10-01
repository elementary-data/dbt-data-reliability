{% macro normalize_data_type(data_type) %}

    {# In case data type has precision info - e.g. decimal is in the format decimal(p,s) #}
    {%- if '(' in data_type %}
        {%- set data_type = data_type.split('(')[0] %}
    {%- endif %}

    {%- if data_type is defined and data_type is not none %}
        {%- if elementary.is_data_type_of_normalized_type(data_type, 'string') %}
            {{ return('string') }}
        {%- elif elementary.is_data_type_of_normalized_type(data_type, 'numeric') %}
            {{ return('numeric') }}
        {%- elif elementary.is_data_type_of_normalized_type(data_type, 'timestamp') %}
            {{ return('timestamp') }}
        {%- elif elementary.is_data_type_of_normalized_type(data_type, "boolean") %}
            {{ return("boolean") }}
        {%- else %}
            {{ return('other') }}
        {% endif %}
    {%- endif %}
{% endmacro %}

{% macro is_data_type_of_normalized_type(data_type, normalized_type) %}
    {% set data_type_list = elementary.data_type_list(normalized_type) | map('lower') %}
    {% do return(data_type | lower in data_type_list) %}
{% endmacro %}
