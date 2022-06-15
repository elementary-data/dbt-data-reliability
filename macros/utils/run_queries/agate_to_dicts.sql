{% macro agate_to_dicts(agate_table) %}
    {% set column_types = agate_table.column_types %}
    {% set serializable_rows = [] %}
    {% for agate_row in agate_table.rows %}
        {% set serializable_row = {} %}
        {% for col_name, col_value in agate_row.items() %}
            {% set serializable_col_value = column_types[loop.index0].jsonify(col_value) %}
            {% set serializable_col_name = col_name | lower %}
            {% do serializable_row.update({serializable_col_name: serializable_col_value}) %}
        {% endfor %}
        {% do serializable_rows.append(serializable_row) %}
    {% endfor %}
    {{ return(serializable_rows) }}
{% endmacro %}