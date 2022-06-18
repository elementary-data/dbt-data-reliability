{% macro get_query_history_from_information_schema(schema_tuple) %}
    {{ return(adapter.dispatch('get_query_history_from_information_schema','elementary')(schema_tuple)) }}
{% endmacro %}

{# Snowflake #}
{% macro default__get_query_history_from_information_schema(schema_tuple) %}
    {%- set query_comment_string = elementary.get_config_var('query_comment_string') %}
    {%- set database_name, schema_name = schema_tuple %}
    {%- set schema_relation = api.Relation.create(database=database_name, schema=schema_name).without_identifier() %}

    (

        with {{ database_name }}_{{ schema_name }}_query_history as (

            select
                *,
                {{ elementary.substr_between_two_strings('query_text', query_comment_string) }} as elementary_comment
            from table( {{ database_name }}.information_schema.query_history())
            where
                    query_type not in
                    ('SHOW', 'COPY', 'COMMIT', 'DESCRIBE', 'ROLLBACK', 'CREATE_STREAM', 'DROP_STREAM', 'PUT_FILES', 'GET_FILES',
                     'BEGIN_TRANSACTION', 'GRANT', 'ALTER_SESSION', 'USE', 'ALTER_NETWORK_POLICY', 'ALTER_ACCOUNT',
                     'ALTER_TABLE_DROP_CLUSTERING_KEY', 'ALTER_USER',  'CREATE_CUSTOMER_ACCOUNT', 'CREATE_NETWORK_POLICY',
                     'CREATE_ROLE', 'CREATE_USER', 'DESCRIBE_QUERY', 'DROP_NETWORK_POLICY', 'DROP_ROLE', 'DROP_USER', 'LIST_FILES',
                     'REMOVE_FILES', 'REVOKE','UNKNOWN', 'DELETE', 'SELECT')
              and query_text not ilike '%.query_history%'
              and (lower(query_text) like lower('%{{ database_name }}%')
                or lower(database_name) = lower('{{ database_name }}'))

        )

        select
            *,
            json_extract_path_text(elementary_comment,'node_unique_id') as node_unique_id,
            json_extract_path_text(elementary_comment,'node_identifier') as node_identifier,
            json_extract_path_text(elementary_comment,'resource_type') as resource_type,
            json_extract_path_text(elementary_comment,'package_name') as package_name,
            json_extract_path_text(elementary_comment,'profile_name') as profile_name,
            json_extract_path_text(elementary_comment,'target_name') as target_name
        from {{ database_name }}_{{ schema_name }}_query_history

    )

{% endmacro %}