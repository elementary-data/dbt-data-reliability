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
GRANT SELECT ON ALL TABLES IN SCHEMA IDENTIFIER($elementary_schema_fqn) TO ROLE IDENTIFIER($elementary_role);
GRANT SELECT ON FUTURE TABLES IN SCHEMA IDENTIFIER($elementary_schema_fqn) TO ROLE IDENTIFIER($elementary_role);
GRANT SELECT ON ALL VIEWS IN SCHEMA IDENTIFIER($elementary_schema_fqn) TO ROLE IDENTIFIER($elementary_role);
GRANT SELECT ON FUTURE VIEWS IN SCHEMA IDENTIFIER($elementary_schema_fqn) TO ROLE IDENTIFIER($elementary_role);

-- Account usage access
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE IDENTIFIER($elementary_role);

-- Information schema access
CREATE OR REPLACE PROCEDURE ELEMENTARY_GRANT_INFO_SCHEMA_ACCESS(database_name STRING, role_name STRING)
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
    BEGIN
      GRANT USAGE,MONITOR ON DATABASE IDENTIFIER(:database_name) TO ROLE IDENTIFIER(:role_name);
      GRANT USAGE,MONITOR ON ALL SCHEMAS IN DATABASE IDENTIFIER(:database_name) TO ROLE IDENTIFIER(:role_name);
      GRANT USAGE,MONITOR ON FUTURE SCHEMAS IN DATABASE IDENTIFIER(:database_name) TO ROLE IDENTIFIER(:role_name);

      GRANT REFERENCES ON ALL TABLES IN DATABASE IDENTIFIER(:database_name) TO ROLE IDENTIFIER(:role_name);
      GRANT REFERENCES ON ALL VIEWS IN DATABASE IDENTIFIER(:database_name) TO ROLE IDENTIFIER(:role_name);
      GRANT REFERENCES ON ALL EXTERNAL TABLES IN DATABASE IDENTIFIER(:database_name) TO ROLE IDENTIFIER(:role_name);

      GRANT REFERENCES ON FUTURE TABLES IN DATABASE IDENTIFIER(:database_name) TO ROLE IDENTIFIER(:role_name);
      GRANT REFERENCES ON FUTURE VIEWS IN DATABASE IDENTIFIER(:database_name) TO ROLE IDENTIFIER(:role_name);
      GRANT REFERENCES ON FUTURE EXTERNAL TABLES IN DATABASE IDENTIFIER(:database_name) TO ROLE IDENTIFIER(:role_name);
    END;
  $$
;

{%- set databases = elementary.get_configured_databases_from_graph()%}
{% for database in databases %}
{#
  'snowflake' database is excluded because it does not support granting individual privileges (we ask for relevant access to it
  via the database roles below)
  see: https://docs.snowflake.com/en/sql-reference/account-usage#enabling-the-snowflake-database-usage-for-other-roles
#}
  {%- if database | lower != 'snowflake' -%}
CALL ELEMENTARY_GRANT_INFO_SCHEMA_ACCESS('{{ database }}', $elementary_role);
  {%- endif -%}
{% endfor %}

-- Query history access
CREATE OR REPLACE PROCEDURE ELEMENTARY_GRANT_QUERY_HISTORY_ACCESS(role_name STRING)
  RETURNS VARCHAR
  LANGUAGE SQL
  AS
  $$
    BEGIN
      GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER TO ROLE IDENTIFIER(:role_name);
      GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER TO ROLE IDENTIFIER(:role_name);
      GRANT DATABASE ROLE SNOWFLAKE.GOVERNANCE_VIEWER TO ROLE IDENTIFIER(:role_name);

      LET warehouses_rs RESULTSET := (SHOW WAREHOUSES);
      LET warehouses_cur CURSOR FOR warehouses_rs;
      FOR warehouse_row IN warehouses_cur DO
        LET warehouse_name VARCHAR := warehouse_row."name";
        GRANT MONITOR ON WAREHOUSE IDENTIFIER(:warehouse_name) TO ROLE IDENTIFIER(:role_name);
      END FOR;
    END;
  $$
;
CALL ELEMENTARY_GRANT_QUERY_HISTORY_ACCESS($elementary_role);
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
{% endmacro %}


{% macro postgres__get_profile_creation_query(parameters) %}
-- Create postgres user
CREATE USER {{ parameters["user"] }} WITH PASSWORD '{{ parameters["password"] }}';

-- Grant read access to the Elementary schema
GRANT USAGE ON SCHEMA {{ parameters["schema"] }} TO {{ parameters["user"] }};
GRANT SELECT ON ALL TABLES IN SCHEMA {{ parameters["schema"] }} TO {{ parameters["user"] }};
ALTER DEFAULT PRIVILEGES IN SCHEMA {{ parameters["schema"] }} GRANT SELECT ON TABLES TO {{ parameters["user"] }};
{% endmacro %}


{# Databricks, BigQuery, Spark #}
{% macro default__get_profile_creation_query(parameters) %}
  {% do exceptions.raise_compiler_error('User creation not supported through sql using ' ~ target.type) %}
{% endmacro %}
