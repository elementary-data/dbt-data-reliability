{#-
    NOTE FOR PACKAGE CONSUMERS:
    This package macro is not guaranteed to be picked up automatically by dbt's
    incremental strategy resolution in all projects.
    To apply this behavior, users should:
      1) Override `get_incremental_microbatch_sql` in their own project and delegate to
         `elementary.capture_and_execute_microbatch_compiled_code_sql(arg_dict)`.
      2) Enable dbt behavior flag `require_batched_execution_for_custom_microbatch_strategy`.

    This flow is currently not supported for adapters:
      - spark
      - bigquery
      - athena
      - clickhouse
      - dremio
      - vertica

    This flow is currently not supported for dbt Fusion.
-#}
{% macro capture_and_execute_microbatch_compiled_code_sql(arg_dict) %}
    {% if execute and model is defined %}
        {% do elementary.capture_microbatch_compiled_code_for_model() %}
    {% endif %}

    {{ return(adapter.dispatch("get_incremental_microbatch_sql", "dbt")(arg_dict)) }}
{% endmacro %}


{% macro capture_microbatch_compiled_code_for_model() %}
    {% set model_unique_id = (
        model.get("unique_id") if model is mapping else model.unique_id
    ) | default(none, true) %}
    {% set model_compiled_code = (
        model.get("compiled_code")
        if model is mapping
        else model.compiled_code
    ) | default(none, true) %}
    {% if model_unique_id is none %} {{ return(none) }} {% endif %}
    {% if not model_compiled_code %} {{ return(none) }} {% endif %}

    {% set compiled_code_by_unique_id = elementary.get_cache(
        "microbatch_compiled_code_by_unique_id"
    ) %}
    {% if model_unique_id in compiled_code_by_unique_id %}
        {{ return(none) }}
    {% endif %}
    {% do compiled_code_by_unique_id.update({model_unique_id: model_compiled_code}) %}
{% endmacro %}
