{% macro get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {% do return(
        adapter.dispatch("get_stale_test_tables", "elementary")(
            elementary_database, elementary_schema, hours, table_name_pattern
        )
    ) %}
{% endmacro %}


{% macro default__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {% do exceptions.raise_compiler_error(
        "elementary.get_stale_test_tables is not implemented for adapter: "
        ~ adapter.type()
    ) %}
{% endmacro %}


{% macro _stale_test_table_rows_to_relations(results) %}
    {% set relations = [] %}
    {% for row in results.rows %}
        {% do relations.append(
            api.Relation.create(
                database=row[0] if row[0] is not none else none,
                schema=row[1],
                identifier=row[2],
                type="table",
            )
        ) %}
    {% endfor %}
    {% do return(relations) %}
{% endmacro %}


{% macro snowflake__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {% set schema_relation = api.Relation.create(
        database=elementary_database, schema=elementary_schema
    ).without_identifier() %}
    {% set query %}
        select table_catalog, table_schema, table_name
        from {{ schema_relation.information_schema("TABLES") }}
        where
            upper(table_schema) = upper('{{ elementary_schema }}')
            and lower(table_name) like '{{ table_name_pattern }}'
            and created < dateadd(hour, -{{ hours | int }}, current_timestamp())
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro bigquery__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {% set schema_relation = api.Relation.create(
        database=elementary_database, schema=elementary_schema
    ).without_identifier() %}
    {% set query %}
        select table_catalog, table_schema, table_name
        from {{ schema_relation.information_schema("TABLES") }}
        where
            upper(table_schema) = upper('{{ elementary_schema }}')
            and lower(table_name) like '{{ table_name_pattern }}'
            and creation_time
            < timestamp_sub(
                current_timestamp(), interval {{ hours | int }} hour
            )
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro redshift__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {# Redshift does not expose table creation time - returning all matching tables #}
    {% do elementary.edr_log_warning(
        "get_stale_test_tables: time-based filtering is not supported on Redshift. "
        ~ "All matching temp tables will be returned regardless of age."
    ) %}
    {% set query %}
        select current_database(), table_schema, table_name
        from pg_catalog.svv_tables
        where
            upper(table_schema) = upper('{{ elementary_schema }}')
            and lower(table_name) like '{{ table_name_pattern }}'
            and table_type = 'BASE TABLE'
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro postgres__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {# Postgres does not expose table creation time - returning all matching tables #}
    {% do elementary.edr_log_warning(
        "get_stale_test_tables: time-based filtering is not supported on Postgres. "
        ~ "All matching temp tables will be returned regardless of age."
    ) %}
    {% set schema_relation = api.Relation.create(
        database=elementary_database, schema=elementary_schema
    ).without_identifier() %}
    {% set query %}
        select table_catalog, table_schema, table_name
        from {{ schema_relation.information_schema("TABLES") }}
        where
            upper(table_schema) = upper('{{ elementary_schema }}')
            and lower(table_name) like '{{ table_name_pattern }}'
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro databricks__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {# Requires Unity Catalog - creation time available in information_schema.TABLES #}
    {% set schema_relation = api.Relation.create(
        database=elementary_database, schema=elementary_schema
    ).without_identifier() %}
    {% set query %}
        select table_catalog, table_schema, table_name
        from {{ schema_relation.information_schema("TABLES") }}
        where
            upper(table_schema) = upper('{{ elementary_schema }}')
            and lower(table_name) like '{{ table_name_pattern }}'
            and created
            < timestampadd(hour, -{{ hours | int }}, current_timestamp())
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro fabric__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {% set query %}
        select db_name(), schema_name(schema_id), name
        from sys.tables
        where
            upper(schema_name(schema_id)) = upper('{{ elementary_schema }}')
            and lower(name) like '{{ table_name_pattern }}'
            and create_date < dateadd(hour, -{{ hours | int }}, getutcdate())
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro fabricspark__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {{
        return(
            elementary.fabric__get_stale_test_tables(
                elementary_database, elementary_schema, hours, table_name_pattern
            )
        )
    }}
{% endmacro %}


{% macro sqlserver__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {% set query %}
        select db_name(), schema_name(schema_id), name
        from sys.tables
        where
            upper(schema_name(schema_id)) = upper('{{ elementary_schema }}')
            and lower(name) like '{{ table_name_pattern }}'
            and create_date < dateadd(hour, -{{ hours | int }}, getutcdate())
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro vertica__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {% set query %}
        select null, table_schema, table_name
        from v_catalog.tables
        where
            upper(table_schema) = upper('{{ elementary_schema }}')
            and lower(table_name) like '{{ table_name_pattern }}'
            and create_time
            < (current_timestamp - interval '{{ hours | int }} hours')
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro clickhouse__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {% set query %}
        select null, database, name
        from system.tables
        where
            upper(database) = upper('{{ elementary_schema }}')
            and lower(name) like '{{ table_name_pattern }}'
            and metadata_modification_time
            <= now() - toIntervalMinute({{ (hours | float * 60) | int }})
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro athena__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {# Athena does not expose table creation time - returning all matching tables #}
    {% do elementary.edr_log_warning(
        "get_stale_test_tables: time-based filtering is not supported on Athena. "
        ~ "All matching temp tables will be returned regardless of age."
    ) %}
    {% set schema_relation = api.Relation.create(
        database=elementary_database, schema=elementary_schema
    ).without_identifier() %}
    {% set query %}
        select table_catalog, table_schema, table_name
        from {{ schema_relation.information_schema("TABLES") }}
        where
            upper(table_schema) = upper('{{ elementary_schema }}')
            and lower(table_name) like '{{ table_name_pattern }}'
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro trino__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {# Trino does not expose table creation time - returning all matching tables #}
    {% do elementary.edr_log_warning(
        "get_stale_test_tables: time-based filtering is not supported on Trino. "
        ~ "All matching temp tables will be returned regardless of age."
    ) %}
    {% set schema_relation = api.Relation.create(
        database=elementary_database, schema=elementary_schema
    ).without_identifier() %}
    {% set query %}
        select table_catalog, table_schema, table_name
        from {{ schema_relation.information_schema("TABLES") }}
        where
            upper(table_schema) = upper('{{ elementary_schema }}')
            and lower(table_name) like '{{ table_name_pattern }}'
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro duckdb__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {# DuckDB does not expose table creation time - returning all matching tables.
       Uses plain information_schema.tables to avoid uppercase path issues in
       DuckDB's in-memory catalog. #}
    {% do elementary.edr_log_warning(
        "get_stale_test_tables: time-based filtering is not supported on DuckDB. "
        ~ "All matching temp tables will be returned regardless of age."
    ) %}
    {% set query %}
        select table_catalog, table_schema, table_name
        from information_schema.tables
        where
            upper(table_schema) = upper('{{ elementary_schema }}')
            and lower(table_name) like '{{ table_name_pattern }}'
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}

{% macro dremio__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern
) %}
    {# Dremio does not expose table creation time - returning all matching tables #}
    {% do elementary.edr_log_warning(
        "get_stale_test_tables: time-based filtering is not supported on Dremio. "
        ~ "All matching temp tables will be returned regardless of age."
    ) %}
    {% set query %}
        select table_catalog, table_schema, table_name
        from INFORMATION_SCHEMA."TABLES"
        where
            upper(table_schema) = upper('{{ elementary_schema }}')
            and lower(table_name) like '{{ table_name_pattern }}'
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}
