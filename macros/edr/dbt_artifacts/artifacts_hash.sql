{% macro get_ordered_artifact_hash_query(model) %}
select artifact_hash from {{ ref(model) }}
order by artifact_hash
{% endmacro %}
