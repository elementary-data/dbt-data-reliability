{% macro upload_csv_to_relation(relation, csv_path) %}
  {% do adapter.dispatch('upload_csv_to_relation', 'elementary')(relation, csv_path) %}
{% endmacro %}

{% macro default__upload_csv_to_relation(relation, csv_path) %}
  {% set table = elementary.get_agate_table().from_csv(csv_path) %}
  {% do adapter.truncate_relation(relation) %}
  {% do elementary.insert_table(relation, table) %}
{% endmacro %}

{% macro bigquery__upload_csv_to_relation(relation, csv_path) %}
  {% set kwargs = {
    'allow_quoted_newlines': true,
    'source_format': 'CSV',
    'autodetect': true,
    'write_disposition': 'WRITE_TRUNCATE',
    'skip_leading_rows': 1
  } %}
  {% do adapter.upload_file(csv_path, relation.database, relation.schema, relation.identifier, kwargs=kwargs) %}
{% endmacro %}
