{% macro agate_to_dicts(agate_table) %}
    {% set rows = namespace(data=none) %}
    {% if elementary.is_dbt_fusion() %}
        {% set rows.data = agate_table %}
    {% else %}
        {% set rows.data = agate_table.rows %}
    {% endif %}

    {% set column_types = agate_table.column_types %}
    {% set serializable_rows = [] %}
    {% for agate_row in rows.data %}
        {% set serializable_row = {} %}
        {% for col_name, col_value in agate_row.items() %}
            {% set serializable_col_value = elementary.agate_val_serialize(col_value) %}
            {% set serializable_col_name = col_name | lower %}
            {% do serializable_row.update({serializable_col_name: serializable_col_value}) %}
        {% endfor %}
        {% do serializable_rows.append(serializable_row) %}
    {% endfor %}
    {{ return(serializable_rows) }}
{% endmacro %}

{% macro agate_val_serialize(val) %}
  {% if val.year is defined %}
    {% do return(val.isoformat()) %}
  {% endif %}
  {% do return(val) %}
{% endmacro %}