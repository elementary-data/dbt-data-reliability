{% macro get_profile_creation_query(parameters) %}
  {% do return(adapter.dispatch("get_profile_creation_query", "elementary")(parameters)) %}
{% endmacro %}


{% macro snowflake__get_profile_creation_query(parameters) %}
-- Set credentials as variables
SET elementary_database = '{{ parameters["database"] }}';
SET elementary_schema = '{{ parameters["schema"] }}';
SET elementary_warehouse = '{{ parameters["warehouse"] }}';
SET elementary_role = '{{ parameters["role"] }}';
SET elementary_username = '{{ parameters["user"] }}';
SET elementary_password = '{{ parameters["password"] }}';

-- Account admin role required to set up permissions below
USE ROLE ACCOUNTADMIN;

-- Create elementary user and role
CREATE OR REPLACE USER IDENTIFIER($elementary_username) PASSWORD = $elementary_password;
CREATE OR REPLACE ROLE IDENTIFIER($elementary_role);
GRANT ROLE IDENTIFIER($elementary_role) TO USER IDENTIFIER($elementary_username);

-- Grant elementary role access to the supplied warehouse
GRANT USAGE ON WAREHOUSE IDENTIFIER($elementary_warehouse) TO ROLE IDENTIFIER($elementary_role);

-- Read access to elementary schema
SET elementary_schema_fqn = $elementary_database || '.' || $elementary_schema;
GRANT USAGE ON DATABASE IDENTIFIER($elementary_database) TO ROLE IDENTIFIER($elementary_role);
GRANT USAGE ON SCHEMA IDENTIFIER($elementary_schema_fqn) TO ROLE IDENTIFIER($elementary_role);
GRANT SELECT ON ALL TABLES IN SCHEMA IDENTIFIER($elementary_schema_fqn) TO ROLE IDENTIFIER($elementary_role);
GRANT SELECT ON FUTURE TABLES IN SCHEMA IDENTIFIER($elementary_schema_fqn) TO ROLE IDENTIFIER($elementary_role);
GRANT SELECT ON ALL VIEWS IN SCHEMA IDENTIFIER($elementary_schema_fqn) TO ROLE IDENTIFIER($elementary_role);
GRANT SELECT ON FUTURE VIEWS IN SCHEMA IDENTIFIER($elementary_schema_fqn) TO ROLE IDENTIFIER($elementary_role);

-- Account usage access (these permissions allow Elementary to access table & column metadata and query history)
GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER TO ROLE IDENTIFIER($elementary_role);
GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER TO ROLE IDENTIFIER($elementary_role);
GRANT DATABASE ROLE SNOWFLAKE.GOVERNANCE_VIEWER TO ROLE IDENTIFIER($elementary_role);
{% endmacro %}


{% macro redshift__get_profile_creation_query(parameters) %}
-- Create redshift user with unrestricted access to query history (allows Elementary to see queries generated by
-- any user)
CREATE USER {{ parameters["user"] }} WITH PASSWORD '{{ parameters["password"] }}' SYSLOG ACCESS UNRESTRICTED;

-- Grant read access to the Elementary schema
GRANT USAGE ON SCHEMA {{ parameters["schema"] }} TO {{ parameters["user"] }};
GRANT SELECT ON ALL TABLES IN SCHEMA {{ parameters["schema"] }} TO {{ parameters["user"] }};
ALTER DEFAULT PRIVILEGES IN SCHEMA {{ parameters["schema"] }} GRANT SELECT ON TABLES TO {{ parameters["user"] }};

-- Grant metadata access to tables in the warehouse
GRANT SELECT ON svv_table_info to {{ parameters["user"] }};

-- Grant access to columns information (svv_columns) in the warehouse
GRANT SELECT ON pg_catalog.svv_columns to {{ parameters["user"] }};

-- Create stored procedure for granting USAGE privilege for all schemas for metadata access
CREATE OR REPLACE PROCEDURE elementary_grant_usage_on_all_schemas(user_name VARCHAR)
LANGUAGE plpgsql
AS $$
DECLARE
    schema_name RECORD;
BEGIN
    -- Loop through all schemas in the current database
    FOR schema_name IN 
        SELECT nspname 
        FROM pg_namespace 
        WHERE nspname NOT IN ('pg_catalog', 'information_schema') -- Exclude system schemas
    LOOP
        -- Grant USAGE privilege on each schema to the specified user
        IF schema_name.nspname NOT IN ('pg_automv', 'pg_auto_copy', 'pg_s3', 'pg_mv') AND NOT CHARINDEX('/', schema_name.nspname) THEN
            EXECUTE 'GRANT USAGE ON SCHEMA ' || schema_name.nspname || ' TO ' || user_name;
        END IF;
    END LOOP;
END;
$$;

-- Call the procedure to grant USAGE on all schemas to the specified user
CALL elementary_grant_usage_on_all_schemas('{{ parameters["user"] }}');
{% endmacro %}


{% macro postgres__get_profile_creation_query(parameters) %}
-- Create postgres user
CREATE USER {{ parameters["user"] }} WITH PASSWORD '{{ parameters["password"] }}';

-- Grant read access to the Elementary schema
GRANT USAGE ON SCHEMA {{ parameters["schema"] }} TO {{ parameters["user"] }};
GRANT SELECT ON ALL TABLES IN SCHEMA {{ parameters["schema"] }} TO {{ parameters["user"] }};
ALTER DEFAULT PRIVILEGES IN SCHEMA {{ parameters["schema"] }} GRANT SELECT ON TABLES TO {{ parameters["user"] }};
{% endmacro %}


{% macro clickhouse__get_profile_creation_query(parameters) %}
-- Create clickhouse user
CREATE USER {{ parameters["user"] }} identified by '{{ parameters["password"] }}';

-- Grant select on all tables in the Elementary schema to the user
grant select on {{ parameters["schema"] }}.* to {{ parameters["user"] }}
{% endmacro %}


{# Databricks, BigQuery, Spark #}
{% macro default__get_profile_creation_query(parameters) %}
  {% do exceptions.raise_compiler_error('User creation not supported through sql using ' ~ target.type) %}
{% endmacro %}
