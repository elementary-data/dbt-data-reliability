{% macro result_row_to_dict(single_column_query) %}

    {% set query = single_column_query %}

    {% set results = run_query(query) %}
    {%- if results %}
        {%- for result in results %}
            {%- set dict = result.dict() %}
            {{ return(dict) }}
        {%- endfor %}
    {% endif %}

    {{ return(null) }}

{% endmacro %}