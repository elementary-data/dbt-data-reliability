{% macro get_profile_creation_query(parameters) %}
    {% do return(
        adapter.dispatch("get_profile_creation_query", "elementary")(
            parameters
        )
    ) %}
{% endmacro %}


{% macro snowflake__get_profile_creation_query(parameters) %}
CREATE OR REPLACE USER {{ parameters["user"] }} PASSWORD = '{{ parameters["password"] }}';
CREATE OR REPLACE ROLE {{ parameters["role"] }};
    grant role {{ parameters["role"] }}
    to user {{ parameters["user"] }}
    ;
    grant usage
    on warehouse {{ parameters["warehouse"] }}
    to role {{ parameters["role"] }}
    ;
    grant usage
    on database {{ parameters["database"] }}
    to role {{ parameters["role"] }}
    ;
    grant usage
    on schema {{ parameters["database"] }}.{{ parameters["schema"] }}
    to role {{ parameters["role"] }}
    ;
    grant select
    on all tables in schema {{ parameters["database"] }}.{{ parameters["schema"] }}
    to role {{ parameters["role"] }}
    ;
    grant select
    on future tables in schema {{ parameters["database"] }}.{{ parameters["schema"] }}
    to role {{ parameters["role"] }}
    ;
    grant select
    on all views in schema {{ parameters["database"] }}.{{ parameters["schema"] }}
    to role {{ parameters["role"] }}
    ;
    grant select
    on future views in schema {{ parameters["database"] }}.{{ parameters["schema"] }}
    to role {{ parameters["role"] }}
    ;
{% endmacro %}


{% macro postgres__get_profile_creation_query(parameters) %}
CREATE USER {{ parameters["user"] }} WITH PASSWORD '{{ parameters["password"] }}';
    grant usage
    on schema {{ parameters["schema"] }}
    to {{ parameters["user"] }}
    ;
    grant select
    on all tables in schema {{ parameters["schema"] }}
    to {{ parameters["user"] }}
    ;
ALTER DEFAULT PRIVILEGES IN SCHEMA {{ parameters["schema"] }} GRANT SELECT ON TABLES TO
 {{ parameters["user"] }};
{% endmacro %}


{# Databricks, BigQuery, Spark #}
{% macro default__get_profile_creation_query(parameters) %}
    {% do exceptions.raise_compiler_error(
        "User creation not supported through sql using " ~ target.type
    ) %}
{% endmacro %}
