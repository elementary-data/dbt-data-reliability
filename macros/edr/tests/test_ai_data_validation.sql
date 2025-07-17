{% test ai_data_validation(model, column_name, expectation_prompt, llm_model_name=none, prompt_context='') %}
    {{ config(tags = ['elementary-tests']) }}
    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}
       {% set model_relation = elementary.get_model_relation_for_test(model, elementary.get_test_model()) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("Unsupported model: " ~ model ~ " (this might happen if you override 'ref' or 'source')") }}
        {% endif %}
        
        {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}

        {# Prompt to supply to the LLM #}
        {% set prompt_context_part = prompt_context ~ " " if prompt_context else "" %}
        {% set prompt_template = "You are a data validator that should reply with string true if the expectation is met or the string false otherwise. " ~ prompt_context_part ~ "You got the following expectation: " ~ expectation_prompt ~ ". Your only role is to determine if the following text meets this expectation: " %}

        {{ elementary.generate_ai_data_validation_sql(model, column_name, prompt_template, llm_model_name) }}

    {%- else %}

        {#- test must run an sql query -#}
        {{ elementary.no_results_query() }}

    {%- endif %}
{% endtest %}


{% macro generate_ai_data_validation_sql(model, column_name, prompt_template, llm_model_name) %}
    {{ return(adapter.dispatch('generate_ai_data_validation_sql', 'elementary')(model, column_name, prompt_template, llm_model_name)) }}
{% endmacro %}

{% macro default__generate_ai_data_validation_sql(model, column_name, prompt_template, llm_model_name) %}
    {{ exceptions.raise_compiler_error("AI data validation is not supported for target: " ~ target.type) }}
{% endmacro %}

{% macro snowflake__generate_ai_data_validation_sql(model, column_name, prompt_template, llm_model_name) %}
    {% set default_snowflake_model_name = 'claude-3-5-sonnet' %}
    {% set chosen_llm_model_name = llm_model_name if llm_model_name is not none and llm_model_name|trim != '' else default_snowflake_model_name %}
    
    with ai_data_validation_results as (
        select 
            snowflake.cortex.complete(
                '{{ chosen_llm_model_name }}',
                concat('{{ prompt_template }}', {{ column_name }}::text)
            ) as result
        from {{ model }}
    )

    select *
    from ai_data_validation_results
    where lower(result) like '%false%'
{% endmacro %}

{% macro databricks__generate_ai_data_validation_sql(model, column_name, prompt_template, llm_model_name) %}
    {% set default_databricks_model_name = 'databricks-meta-llama-3-3-70b-instruct' %}
    {% set chosen_llm_model_name = llm_model_name if llm_model_name is not none and llm_model_name|trim != '' else default_databricks_model_name %}
    
    with ai_data_validation_results as (
        select 
            ai_query(
                '{{ chosen_llm_model_name }}',
                concat('{{ prompt_template }}', cast({{ column_name }} as string))
            ) as result
        from {{ model }}
    )

    select *
    from ai_data_validation_results
    where lower(result) like '%false%'
{% endmacro %}


{% macro bigquery__generate_ai_data_validation_sql(model, column_name, prompt_template, llm_model_name) %}
    {% set default_bigquery_model_name = 'gemini-1.5-pro' %}
    {% set chosen_llm_model_name = llm_model_name if llm_model_name is not none and llm_model_name|trim != '' else default_bigquery_model_name %}
    
    with ai_data_validation_results as (
        SELECT ml_generate_text_llm_result as result
        FROM
        ML.GENERATE_TEXT(
            MODEL `{{model.schema}}.{{chosen_llm_model_name}}`,
            (
            SELECT
                CONCAT(
                '{{ prompt_template }}',
                {{column_name}}) AS prompt
            FROM {{model}}),
            STRUCT(TRUE AS flatten_json_output))
    )

    select *
    from ai_data_validation_results
    where lower(result) like '%false%'
{% endmacro %}