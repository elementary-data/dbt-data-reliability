{% test validate_unstructured_data(model, column_name, expectation_prompt, llm_model_name='claude-3-5-sonnet') %}
    {{ config(tags = ['elementary-tests']) }}
    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}
       {% set model_relation = elementary.get_model_relation_for_test(model, context["model"]) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("Unsupported model: " ~ model ~ " (this might happen if you override 'ref' or 'source')") }}
        {% endif %}
        
        {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}

        {# Prompt to supply to the LLM #}
        {% set prompt_template = "You are an unstructred data validator that should reply with string true if the expectation is met or the string false otherwise. You got the following expectation: " ~ expectation_prompt ~ ". Your only role is to determine if the following text meets this expectation: "%}

        {{ elementary.generate_unstructured_data_validation(model, column_name, prompt_template, llm_model_name) }}

    {%- else %}

        {#- test must run an sql query -#}
        {{ elementary.no_results_query() }}

    {%- endif %}
{% endtest %}


{% macro generate_unstructured_data_validation(model, column_name, prompt_template, llm_model_name) %}
    {{ return(adapter.dispatch('generate_unstructured_data_validation', 'elementary')(model, column_name, prompt_template, llm_model_name)) }}
{% endmacro %}

{% macro default__generate_unstructured_data_validation(model, column_name, prompt_template, llm_model_name) %}
    {{ exceptions.raise_compiler_error("Unstructured data validation is not supported for target: " ~ target.type) }}
{% endmacro %}

{% macro snowflake__generate_unstructured_data_validation(model, column_name, prompt_template, llm_model_name) %}
    with unstructured_data_validation as (
        select 
            snowflake.cortex.complete(
                '{{ llm_model_name }}',
                concat('{{ prompt_template }}', {{ column_name }}::text)
            ) as result
        from {{ model }}
    )

    select *
    from unstructured_data_validation
    where lower(result) like '%false%'
{% endmacro %}

{% macro databricks__generate_unstructured_data_validation(model, column_name, prompt_template, llm_model_name='databricks-meta-llama-3-3-70b-instruct') %}
    with unstructured_data_validation as (
        select 
            ai_query(
                '{{ llm_model_name }}',
                concat('{{ prompt_template }}', cast({{ column_name }} as string))
            ) as result
        from {{ model }}
    )

    select *
    from unstructured_data_validation
    where lower(result) like '%false%'
{% endmacro %}


{% macro bigquery__generate_unstructured_data_validation(model, column_name, prompt_template, llm_model_name='flash15_model') %}
    with unstructured_data_validation as (
        SELECT ml_generate_text_llm_result as result
        FROM
        ML.GENERATE_TEXT(
            MODEL `{{model.schema}}.{{llm_model_name}}`,
            (
            SELECT
                CONCAT(
                '{{ prompt_template }}',
                {{column_name}}) AS prompt
            FROM {{model}}),
            STRUCT(TRUE AS flatten_json_output))
    )

    select *
    from unstructured_data_validation
    where lower(result) like '%false%'
{% endmacro %}