{% macro upload_run_results(results) %}
    {% if elementary.get_config_var('disable_run_results') %}
        {% do elementary.edr_log("Run results are disabled, skipping upload.") %}
        {{ return('') }}
    {% endif %}
    {{ elementary.edr_log("Uploading run results.") }}
    {% set edr_cli_run = elementary.get_config_var('edr_cli_run') %}
    {% if execute and not edr_cli_run and results %}
        {% set database_name, schema_name = elementary.get_model_database_and_schema('elementary', 'dbt_run_results') %}
        {%- set dbt_run_results_relation = adapter.get_relation(database=database_name,
                                                                schema=schema_name,
                                                                identifier='dbt_run_results') -%}
        {%- if dbt_run_results_relation -%}
            {% do elementary.upload_artifacts_to_table(dbt_run_results_relation, results, elementary.get_flatten_run_result_callback(),
                                                       should_commit=True) %}
        {%- endif -%}
    {% endif %}
    {{ elementary.edr_log("Uploaded run results successfully.") }}
    {{ return ('') }}
{% endmacro %}
