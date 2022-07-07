{% macro upload_run_results(results) %}
    {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
    {% if execute and not edr_cli_run and results %}
        {% if 'model.elementary.dbt_run_results' not in graph.nodes %}
            {% do elementary.edr_log("dbt_run_results is disabled, not uploading the run results.") %}
            {{ return('') }}
        {% endif %}

        {% set database_name, schema_name = elementary.get_model_database_and_schema('elementary', 'dbt_run_results') %}
        {%- set dbt_run_results_relation = adapter.get_relation(database=database_name,
                                                                schema=schema_name,
                                                                identifier='dbt_run_results') -%}
        {%- if dbt_run_results_relation -%}
            {% do elementary.upload_artifacts_to_table(dbt_run_results_relation, results, elementary.get_flatten_run_result_callback(),
                                                       should_commit=True) %}
        {%- endif -%}
    {% endif %}
    {{ return ('') }}
{% endmacro %}
