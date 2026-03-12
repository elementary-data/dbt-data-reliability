{% macro full_table_name(alias) -%}
    {{ adapter.dispatch("full_table_name", "elementary")(alias) }}
{%- endmacro %}

{% macro default__full_table_name(alias) -%}
    {% if alias is defined %} {%- set alias_dot = alias ~ "." %} {% endif %}
    upper(
        {{ elementary.edr_concat([alias_dot ~ 'database_name', "'.'", alias_dot ~ 'schema_name', "'.'", alias_dot ~ 'table_name']) }}
    )
{%- endmacro %}

{% macro clickhouse__full_table_name(alias) -%}
    {# ClickHouse uses database=schema, so use 2-part names (schema.table) #}
    {% if alias is defined %} {%- set alias_dot = alias ~ "." %} {% endif %}
    upper({{ alias_dot }}schema_name || '.' || {{ alias_dot }}table_name)
{%- endmacro %}

{% macro vertica__full_table_name(alias) -%}
    {# Vertica: upper() doubles varchar byte-length; cast to varchar(1000) first to stay under 65000 limit #}
    {% if alias is defined %} {%- set alias_dot = alias ~ "." %} {% endif %}
    upper(cast(
        {{ alias_dot }}database_name || '.' || {{ alias_dot }}schema_name || '.' || {{ alias_dot }}table_name
    as varchar(1000)))
{%- endmacro %}


{% macro full_schema_name() -%}
    {{ adapter.dispatch("full_schema_name", "elementary")() }}
{%- endmacro %}

{% macro default__full_schema_name() -%}
    upper({{ elementary.edr_concat(["database_name", "'.'", "schema_name"]) }})
{%- endmacro %}

{% macro clickhouse__full_schema_name() -%}
    {# ClickHouse uses database=schema, so schema_name alone is the full schema name #}
    upper(schema_name)
{%- endmacro %}

{% macro vertica__full_schema_name() -%}
    {# Vertica: upper() doubles varchar byte-length; cast first to stay under 65000 limit #}
    upper(cast(database_name || '.' || schema_name as varchar(1000)))
{%- endmacro %}


{% macro full_column_name() -%}
    {{ adapter.dispatch("full_column_name", "elementary")() }}
{%- endmacro %}

{% macro default__full_column_name() -%}
    upper(
        {{ elementary.edr_concat(["database_name", "'.'", "schema_name", "'.'", "table_name", "'.'", "column_name"]) }}
    )
{%- endmacro %}

{% macro clickhouse__full_column_name() -%}
    {# ClickHouse uses database=schema, so use schema.table.column #}
    upper(schema_name || '.' || table_name || '.' || column_name)
{%- endmacro %}

{% macro vertica__full_column_name() -%}
    {# Vertica: upper() doubles varchar byte-length; cast first to stay under 65000 limit #}
    upper(cast(
        database_name || '.' || schema_name || '.' || table_name || '.' || column_name
    as varchar(1000)))
{%- endmacro %}


{% macro full_name_split(part_name) %}
    {{ adapter.dispatch("full_name_split", "elementary")(part_name) }}
{% endmacro %}

{% macro vertica__full_name_split(part_name) %}
    {# Vertica supports split_part (1-based index) but not array subscript syntax #}
    {%- if part_name == "database_name" -%} {%- set part_index = 1 -%}
    {%- elif part_name == "schema_name" -%} {%- set part_index = 2 -%}
    {%- elif part_name == "table_name" -%} {%- set part_index = 3 -%}
    {%- else -%} {{ return("") }}
    {%- endif -%}
    trim(both '"' from split_part(full_table_name, '.', {{ part_index }})) as {{ part_name }}
{% endmacro %}


{% macro default__full_name_split(part_name) %}
    {%- if part_name == "database_name" -%} {%- set part_index = 0 -%}
    {%- elif part_name == "schema_name" -%} {%- set part_index = 1 -%}
    {%- elif part_name == "table_name" -%} {%- set part_index = 2 -%}
    {%- else -%} {{ return("") }}
    {%- endif -%}
    trim(split(full_table_name, '.')[{{ part_index }}], '"') as {{ part_name }}
{% endmacro %}


{% macro clickhouse__full_name_split(part_name) %}
    {# ClickHouse full_table_name is 2-part: schema.table #}
    {%- if part_name == "database_name" -%}
        {# database = schema in ClickHouse #}
        trim(both '"' from splitByChar('.', full_table_name)[1]) as {{ part_name }}
    {%- elif part_name == "schema_name" -%}
        trim(both '"' from splitByChar('.', full_table_name)[1]) as {{ part_name }}
    {%- elif part_name == "table_name" -%}
        trim(both '"' from splitByChar('.', full_table_name)[2]) as {{ part_name }}
    {%- else -%} {{ return("") }}
    {%- endif -%}
{% endmacro %}


{% macro fabric__full_name_split(part_name) %}
    {# T-SQL: use PARSENAME which splits dotted names (parts numbered right-to-left).
       PARSENAME returns nvarchar which Fabric does not support, so cast to varchar. #}
    {%- if part_name == "database_name" -%} {%- set part_index = 3 -%}
    {%- elif part_name == "schema_name" -%} {%- set part_index = 2 -%}
    {%- elif part_name == "table_name" -%} {%- set part_index = 1 -%}
    {%- else -%} {{ return("") }}
    {%- endif -%}
    cast(replace(parsename(full_table_name, {{ part_index }}), '"', '') as varchar(256)) as {{ part_name }}
{% endmacro %}

{% macro bigquery__full_name_split(part_name) %}
    {%- if part_name == "database_name" -%} {%- set part_index = 0 %}
    {%- elif part_name == "schema_name" -%} {%- set part_index = 1 %}
    {%- elif part_name == "table_name" -%} {%- set part_index = 2 -%}
    {%- else -%} {{ return("") }}
    {%- endif -%}
    trim(split(full_table_name, '.')[offset({{ part_index }})], '"') as {{ part_name }}
{% endmacro %}


{% macro postgres__full_name_split(part_name) %}
    {%- if part_name == "database_name" -%} {%- set part_index = 1 -%}
    {%- elif part_name == "schema_name" -%} {%- set part_index = 2 -%}
    {%- elif part_name == "table_name" -%} {%- set part_index = 3 -%}
    {%- else -%} {{ return("") }}
    {%- endif -%}
    trim(split_part(full_table_name, '.',{{ part_index }}), '"') as {{ part_name }}
{% endmacro %}

{% macro athena__full_name_split(part_name) %}
    {%- if part_name == "database_name" -%} {%- set part_index = 1 -%}
    {%- elif part_name == "schema_name" -%} {%- set part_index = 2 -%}
    {%- elif part_name == "table_name" -%} {%- set part_index = 3 -%}
    {%- else -%} {{ return("") }}
    {%- endif -%}
    trim(split_part(full_table_name, '.',{{ part_index }}), '"') as {{ part_name }}
{% endmacro %}


{% macro duckdb__full_name_split(part_name) %}
    {%- if part_name == "database_name" -%} {%- set part_index = 1 -%}
    {%- elif part_name == "schema_name" -%} {%- set part_index = 2 -%}
    {%- elif part_name == "table_name" -%} {%- set part_index = 3 -%}
    {%- else -%} {{ return("") }}
    {%- endif -%}
    trim(split_part(full_table_name, '.',{{ part_index }}), '"') as {{ part_name }}
{% endmacro %}


{% macro databricks__full_name_split(part_name) %}
    {%- if part_name == "database_name" -%} {%- set part_index = 0 -%}
    {%- elif part_name == "schema_name" -%} {%- set part_index = 1 -%}
    {%- elif part_name == "table_name" -%} {%- set part_index = 2 -%}
    {%- else -%} {{ return("") }}
    {%- endif -%}
    trim('"' from split(full_table_name, '[.]')[{{ part_index }}]) as {{ part_name }}
{% endmacro %}


{% macro trino__full_name_split(part_name) %}
    {# Trino arrays are 1-based, so we use 1/2/3 instead of 0/1/2 #}
    {%- if part_name == "database_name" -%} {%- set part_index = 1 -%}
    {%- elif part_name == "schema_name" -%} {%- set part_index = 2 -%}
    {%- elif part_name == "table_name" -%} {%- set part_index = 3 -%}
    {%- else -%} {{ return("") }}
    {%- endif -%}
    trim(both '"' from split(full_table_name, '.')[{{ part_index }}]) as {{ part_name }}
{% endmacro %}


{% macro dremio__full_name_split(part_name) %}
    {%- if part_name == "database_name" -%}
        trim('"' from split_part(full_table_name, '.', 1)) as {{ part_name }}
    {%- elif part_name == "schema_name" -%}
        trim(
            '"'
            from
                substr(
                    full_table_name,
                    length(split_part(full_table_name, '.', 1)) + 2,
                    length(full_table_name)
                    - length(split_part(full_table_name, '.', 1))
                    - length(
                        split_part(
                            full_table_name,
                            '.',
                            length(full_table_name)
                            - length(replace(full_table_name, '.', ''))
                            + 1
                        )
                    )
                    - 2
                )
        ) as {{ part_name }}
    {%- elif part_name == "table_name" -%}
        trim(
            '"'
            from
                split_part(
                    full_table_name,
                    '.',
                    length(full_table_name)
                    - length(replace(full_table_name, '.', ''))
                    + 1
                )
        ) as {{ part_name }}
    {%- else -%} {{ return("") }}
    {%- endif -%}
{% endmacro %}


{% macro relation_to_full_name(relation) %}
    {{ return(adapter.dispatch("relation_to_full_name", "elementary")(relation)) }}
{% endmacro %}

{% macro default__relation_to_full_name(relation) %}
    {%- if relation.is_cte %}
        {# Ephemeral models don't have db and schema #}
        {%- set full_table_name = relation.identifier | upper %}
    {%- elif relation.database %}
        {%- set full_table_name = (
            relation.database
            | upper ~ "." ~ relation.schema
            | upper ~ "." ~ relation.identifier
            | upper
        ) %}
    {%- else %}
        {# Databricks doesn't always have a database #}
        {%- set full_table_name = (
            relation.schema | upper ~ "." ~ relation.identifier | upper
        ) %}
    {%- endif %}
    {{ return(full_table_name) }}
{% endmacro %}

{% macro clickhouse__relation_to_full_name(relation) %}
    {# ClickHouse uses database=schema, so always use 2-part names (schema.table) #}
    {%- if relation.is_cte %} {%- set full_table_name = relation.identifier | upper %}
    {%- else %}
        {%- set full_table_name = (
            relation.schema | upper ~ "." ~ relation.identifier | upper
        ) %}
    {%- endif %}
    {{ return(full_table_name) }}
{% endmacro %}


{% macro model_node_to_full_name(model_node) %}
    {{ return(adapter.dispatch("model_node_to_full_name", "elementary")(model_node)) }}
{% endmacro %}

{% macro default__model_node_to_full_name(model_node) %}
    {% set identifier = model_node.identifier or model_node.alias %}
    {%- if model_node.database %}
        {%- set full_table_name = (
            model_node.database
            | upper ~ "." ~ model_node.schema
            | upper ~ "." ~ identifier
            | upper
        ) %}
    {%- else %}
        {# Databricks doesn't always have a database #}
        {%- set full_table_name = (
            model_node.schema | upper ~ "." ~ identifier | upper
        ) %}
    {%- endif %}
    {{ return(full_table_name) }}
{% endmacro %}

{% macro clickhouse__model_node_to_full_name(model_node) %}
    {# ClickHouse uses database=schema, so always use 2-part names (schema.table) #}
    {% set identifier = model_node.identifier or model_node.alias %}
    {%- set full_table_name = model_node.schema | upper ~ "." ~ identifier | upper %}
    {{ return(full_table_name) }}
{% endmacro %}


{% macro configured_schemas_from_graph_as_tuple() %}
    {{
        return(
            adapter.dispatch("configured_schemas_from_graph_as_tuple", "elementary")()
        )
    }}
{% endmacro %}

{% macro default__configured_schemas_from_graph_as_tuple() %}

    {%- set configured_schema_tuples = elementary.get_configured_schemas_from_graph() %}
    {%- set schemas_list = [] %}

    {%- for configured_schema_tuple in configured_schema_tuples %}
        {%- set database_name, schema_name = configured_schema_tuple %}
        {%- set full_schema_name = database_name | upper ~ "." ~ schema_name | upper %}
        {%- do schemas_list.append(full_schema_name) -%}
    {%- endfor %}

    {% set schemas_tuple = elementary.strings_list_to_tuple(schemas_list) %}
    {{ return(schemas_tuple) }}

{% endmacro %}

{% macro clickhouse__configured_schemas_from_graph_as_tuple() %}
    {# ClickHouse uses database=schema, so use just schema_name #}
    {%- set configured_schema_tuples = elementary.get_configured_schemas_from_graph() %}
    {%- set schemas_list = [] %}

    {%- for configured_schema_tuple in configured_schema_tuples %}
        {%- set database_name, schema_name = configured_schema_tuple %}
        {%- set full_schema_name = schema_name | upper %}
        {%- do schemas_list.append(full_schema_name) -%}
    {%- endfor %}

    {% set schemas_tuple = elementary.strings_list_to_tuple(schemas_list) %}
    {{ return(schemas_tuple) }}

{% endmacro %}
