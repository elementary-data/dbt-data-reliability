{% macro result_column_to_list(single_column_query) %}
    {% set results_list = [] %}

    {%- if 'select' in single_column_query %}
        {% call statement('get_query_results', fetch_result=True,auto_begin=false) %}
            {{ single_column_query }}
        {% endcall %}

        {% if execute %}
            {% set results_rows = load_result('get_query_results').table.rows.values() %}
            {%- if results_rows %}
                {% for result_row in results_rows %}
                    {{ results_list.append(results_rows[loop.index0].values()[0]) }}
                {% endfor %}
            {%- endif %}
        {% endif %}
    {%- endif %}

    {{ return(results_list) }}

{% endmacro %}