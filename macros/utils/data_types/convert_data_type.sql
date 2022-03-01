{% macro convert_data_type(data_type) %}
    {%- if data_type %}
        {%- if data_type in elementary.data_type_list('string') %}
            {{ return('string') }}
        {%- elif data_type in elementary.data_type_list('numeric') %}
            {{ return('numeric') }}
        {%- else %}
            {{ return('other') }}
        {% endif %}
    {%- endif %}
{% endmacro %}

