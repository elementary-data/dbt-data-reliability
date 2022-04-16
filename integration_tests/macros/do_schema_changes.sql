{% macro do_schema_changes() %}

    {% set schema_changes_query -%}

    {# column_removed #}
    alter table {{ ref('string_column_anomalies') }}
    drop column min_length;

    {# column_added #}
    alter table {{ ref('string_column_anomalies') }}
    add new_column varchar(100);

    {# column_type_change #}
    alter table {{ ref('string_column_anomalies') }}
    drop column max_length;
    alter table {{ ref('string_column_anomalies') }}
    add max_length integer;

    {# table_removed #}
    drop view {{ ref('numeric_column_anomalies') }};

    {%- endset %}
    {% do elementary.edr_log(schema_changes_query) %}

    {% do run_query(schema_changes_query) %}

{% endmacro %}

