{% macro run_query(query, lowercase_column_names=True) %}
    {{ return(adapter.dispatch('run_query', 'elementary')(query, lowercase_column_names)) }}
{% endmacro %}

{% macro default__run_query(query, lowercase_column_names=True) %}
    {% set query_result = dbt.run_query(query) %}
    {% if lowercase_column_names %}
        {% set lowercased_column_names = {} %}
        {% for column_name in query_result.column_names %}
            {% do lowercased_column_names.update({column_name: column_name.lower()}) %}
        {% endfor %}
        {% set query_result = query_result.rename(lowercased_column_names) %}
    {% endif %}

    {% do return(query_result) %}
{% endmacro %}

{% macro glue__run_query(query, lowercase_column_names=True) %}
    -- Glue does not support running queries that does not return results through the `run_query` method
    -- We need to check if the query is a DDL or DML statement and run it using the `run_query_statement` statement
    -- There are other statements that should not get query results, but keeping it simple for now

    {#{{ log("want to run "~query, info=true) }}#}

    {% set should_not_get_query_results_statements = ["create table", "create or replace table", "insert into", "create view", "create temporary view", "create or replace temporary view"] %}

    {% set should_not_get_query_results = [] %}
    {% for statement in should_not_get_query_results_statements %}
        {% if statement in query.lower() %}
            {% do should_not_get_query_results.append(True) %}
        {% endif %}
    {% endfor %}

    {% if should_not_get_query_results | length > 0 %}
        -- Morover, Glue throws an error if the query contains escaped single quotes in single quotes strings
        {% set curracted_query = query.replace("\\'","") %}

        {% call statement("run_query_statement", fetch_result=false, auto_begin=false) %}
            {{ curracted_query }}
        {% endcall %}

    {% else %}
        {% set query_result = dbt.run_query(query) %}
        
        {% if lowercase_column_names %}
            {% set lowercased_column_names = {} %}
            {% for column_name in query_result.column_names %}
                {% do lowercased_column_names.update({column_name: column_name.lower()}) %}
            {% endfor %}
            {% set query_result = query_result.rename(lowercased_column_names) %}
        {% endif %}

        {% do return(query_result) %}
        {% endif %}
        
{% endmacro %}

