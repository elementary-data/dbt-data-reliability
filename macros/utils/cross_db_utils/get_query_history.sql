{% macro get_query_history(from_time) %}
  {% do return(adapter.dispatch("get_query_history", "elementary")(from_time)) %}
{% endmacro %}

{% macro bigquery__get_query_history(from_time) %}
select 
  job_id || referenced_table.project_id || referenced_table.dataset_id || referenced_table.table_id as id,
  job_id as job_id,
  statement_type,
  start_time,
  end_time,
  state,
  referenced_table.project_id as source_db,
  referenced_table.dataset_id as source_schema,
  referenced_table.table_id as source_table,
  destination_table.project_id as dest_db,
  destination_table.dataset_id as dest_schema,
  destination_table.table_id as dest_table
from `{{ target.project }}`.`region-{{ target.location }}`.INFORMATION_SCHEMA.JOBS, 
  UNNEST(referenced_tables) AS referenced_table
where
  {% if from_time %}
    creation_time > {{ from_time }} and
  {% endif %}
  job_type = 'QUERY'
{% endmacro %}

{# TODO: implement for other DWH types after POC with BigQuery #}
{% macro default__get_query_history(from_time) %}
  {{ elementary.empty_query_history() }}
{% endmacro %}
