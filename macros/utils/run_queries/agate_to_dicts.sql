{% macro agate_to_dicts(agate_table) %}
    {% set rows = namespace(data=none) %}
    {% if elementary.is_dbt_fusion() %}
        {% set rows.data = agate_table %}
    {% else %}
        {% set rows.data = agate_table.rows %}
    {% endif %}

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
  {% if elementary.edr_is_decimal(val) %}
    {% do return(elementary.edr_serialize_decimal(val)) %}
  {% endif %}
  {% do return(val) %}
{% endmacro %}

{% macro edr_is_decimal(val) %}
  {# A hacky way to check if a value is of type Decimal, as there isn't a straightforward way to check that #}
  {% do return(val is number and val.normalize is defined and val.normalize is not none) %}
{% endmacro %}

{% macro edr_serialize_decimal(val) %}
  {% set dec_tuple = val.normalize().as_tuple() %}

  {# A hacky way to standardize Decimals which are not JSON-serializable #}
  {% if dec_tuple[2] == 0 %}
    {% do return(val | string | int) %}
  {% else %}
    {% do return(val | string | float) %}
  {% endif %}
{% endmacro %}
