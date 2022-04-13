{% macro normalize_data_type(data_type) %}
    {%- if data_type is defined and data_type is not none %}
        {%- if data_type in elementary.data_type_list('string') %}
            {{ return('string') }}
        {%- elif data_type in elementary.data_type_list('numeric') %}
            {{ return('numeric') }}
        {%- elif data_type in elementary.data_type_list('timestamp') %}
            {{ return('timestamp') }}
        {%- else %}
            {{ return('other') }}
        {% endif %}
    {%- endif %}
{% endmacro %}