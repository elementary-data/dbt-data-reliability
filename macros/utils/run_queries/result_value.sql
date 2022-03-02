{% macro result_value(single_column_query) %}

    {% set results_list = [] %}
    {% set query = single_column_query %}

    {% if 'select' in query %}
        {% set results = run_query(query) %}
        {%- if results %}
            {%- do results_list.append(results[0].values()[0]) %}
        {% endif %}
    {% endif %}

    {%- if results_list|length > 0 %}
        {%- set result_value = results_list[0] %}
    {%- else %}
        {%- set result_value = null %}
    {%- endif %}

    {{ return(result_value) }}

{% endmacro %}