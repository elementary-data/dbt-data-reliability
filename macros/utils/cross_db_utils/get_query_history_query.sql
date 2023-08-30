{% macro get_query_history_query(from_time) %}
  {% if not from_time %}
    {% do exceptions.raise_compiler_error("from_time variable must be specified") %}
  {% endif %}
  {% do return(adapter.dispatch("get_query_history_query", "elementary")(from_time)) %}
{% endmacro %}


{% macro bigquery__get_query_history_query(from_time) %}
{% set region_relation = api.Relation.create(database=target.project, schema="region-" ~ target.location).without_identifier() %}
{% set jobs_relation = region_relation.information_schema('JOBS') %}
{% if execute and not elementary.can_query_relation(jobs_relation) %}
  {% do return(elementary.empty_query_history()) %}
{% endif %}
select 
  job_id || referenced_table.project_id || referenced_table.dataset_id || referenced_table.table_id as id,
  job_id,
  statement_type,
  start_time,
  end_time,
  state,
  referenced_table.project_id as src_db,
  referenced_table.dataset_id as src_schema,
  referenced_table.table_id as src_table,
  destination_table.project_id as dest_db,
  destination_table.dataset_id as dest_schema,
  destination_table.table_id as dest_table
from {{ jobs_relation }}, 
  UNNEST(referenced_tables) AS referenced_table
where
  creation_time > {{ from_time }}
  and job_type = 'QUERY'
{% endmacro %}

{# TODO: implement for other DWH types after POC with BigQuery #}
{% macro default__get_query_history_query(from_time) %}
  {{ elementary.empty_query_history() }}
{% endmacro %}
