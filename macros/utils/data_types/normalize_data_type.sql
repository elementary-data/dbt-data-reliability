{% macro normalize_data_type(data_type) %}

    {# In case data type has precision info - e.g. decimal is in the format decimal(p,s) #}
    {%- if '(' in data_type %}
        {%- set data_type = data_type.split('(')[0] %}
    {%- endif %}

    {%- if data_type is defined and data_type is not none %}
        {%- if data_type in elementary.data_type_list('string') %}
            {{ return('string') }}
        {%- elif data_type in elementary.data_type_list('numeric') %}
            {{ return('numeric') }}
        {%- elif data_type in elementary.data_type_list('timestamp') %}
            {{ return('timestamp') }}
        {%- elif data_type in elementary.data_type_list("boolean") %}
            {{ return("boolean") }}
        {%- else %}
            {{ return('other') }}
        {% endif %}
    {%- endif %}
{% endmacro %}
