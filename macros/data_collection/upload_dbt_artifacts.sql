{% macro upload_dbt_artifacts(results) %}
    -- depends_on: {{ ref('dbt_run_results') }}
    {% if execute and results %}
        {% set results_list = [] %}
        {% for res in results -%}
            {% do log("unique_id: " ~ res.node.unique_id ~ " database: " ~ res.node.database ~ " schema: " ~ res.node.schema ~ " name: " ~ res.node.name ~ " alias: " ~ res.node.alias ~ " status: " ~ res.status ~ " execution_time: " ~ res.execution_time ~ " started_at: " ~ run_started_at, info=true) %}
            {% do results_list.append({'unique_id': res.node.unique_id, 'database_name': res.node.database, 'schema_name': res.node.schema, 'name': res.node.name, 'alias': res.node.alias, 'status': res.status, 'execution_time': res.execution_time, 'run_started_at': run_started_at}) %}
        {% endfor %}
        {% do insert_dicts_to_table(ref('dbt_run_results'), results_list) %}
    {% endif %}
    {{ return ('') }}
{% endmacro %}
