{% macro get_agate_table() %}
  {% do return(flags.os.sys.modules['agate'].Table) %}
{% endmacro %}

{% macro lowercase_agate_columns(table) %}
  {% set lowercase_column_names = {} %}
  {% for col_name in table.column_names %}
    {% do lowercase_column_names.update({col_name: col_name.lower()}) %}
  {% endfor %}
  {% do return(table.rename(lowercase_column_names)) %}
{% endmacro %}
