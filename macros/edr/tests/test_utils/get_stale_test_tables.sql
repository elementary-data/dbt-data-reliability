{% macro get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern, limit
) %}
    {% do return(
        adapter.dispatch("get_stale_test_tables", "elementary")(
            elementary_database,
            elementary_schema,
            hours,
            table_name_pattern,
            limit,
        )
    ) %}
{% endmacro %}


{% macro default__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern, limit
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
    elementary_database, elementary_schema, hours, table_name_pattern, limit
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
        limit {{ limit }}
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro bigquery__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern, limit
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
        limit {{ limit }}
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro databricks__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern, limit
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
        limit {{ limit }}
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro fabric__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern, limit
) %}
    {% set query %}
        select top {{ limit }} db_name(), schema_name(schema_id), name
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
    elementary_database, elementary_schema, hours, table_name_pattern, limit
) %}
    {{
        return(
            elementary.fabric__get_stale_test_tables(
                elementary_database,
                elementary_schema,
                hours,
                table_name_pattern,
                limit,
            )
        )
    }}
{% endmacro %}


{% macro sqlserver__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern, limit
) %}
    {% set query %}
        select top {{ limit }} db_name(), schema_name(schema_id), name
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
    elementary_database, elementary_schema, hours, table_name_pattern, limit
) %}
    {% set query %}
        select null, table_schema, table_name
        from v_catalog.tables
        where
            upper(table_schema) = upper('{{ elementary_schema }}')
            and lower(table_name) like '{{ table_name_pattern }}'
            and create_time
            < (current_timestamp - interval '{{ hours | int }} hours')
        limit {{ limit }}
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}


{% macro clickhouse__get_stale_test_tables(
    elementary_database, elementary_schema, hours, table_name_pattern, limit
) %}
    {% set query %}
        select null, database, name
        from system.tables
        where
            upper(database) = upper('{{ elementary_schema }}')
            and lower(name) like '{{ table_name_pattern }}'
            and metadata_modification_time
            <= now() - toIntervalMinute({{ (hours | float * 60) | int }})
        limit {{ limit }}
    {% endset %}
    {% if execute %}
        {% set results = elementary.run_query(query) %}
        {% do return(elementary._stale_test_table_rows_to_relations(results)) %}
    {% endif %}
    {% do return([]) %}
{% endmacro %}
