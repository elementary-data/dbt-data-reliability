{#
{% macro upload_dbt_artifacts(results) %}
    {% if execute %}

      {% for res in results -%}
        {% do log("unique_id" ~ res.unique_id ~ "resource_type: " ~ res.resource_type ~ "database: " ~ res.database ~ "schema: " ~ res.schema, info=true) %}
      {% endfor %}

      {% for node in graph.nodes.values() %}
        {% do log(node, info=true) %}
      {% endfor %}
¡
    {% endif %}
{% endmacro %}
#}

{% macro upload_dbt_artifacts(results) %}
    {% if execute %}
        {% set results_list = [] %}
        {% for res in results -%}
            {% do log("unique_id: " ~ res.node.unique_id ~ " database: " ~ res.node.database ~ " schema: " ~ res.node.schema ~ " name: " ~ res.node.name ~ " alias: " ~ res.node.alias ~ " status: " ~ res.status ~ " execution_time: " ~ res.execution_time ~ " started_at: " ~ run_started_at, info=true) %}
            {% do results_list.append({'unique_id': res.node.unique_id, 'database': res.node.database, 'schema': res.node.schema, 'name': res.node.name, 'alias': res.node.alias, 'status': res.status, 'execution_time': res.execution_time, 'run_started_at': run_started_at}) %}
        {% endfor %}
    {% endif %}
{% endmacro %}
