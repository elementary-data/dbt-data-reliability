{% macro do_schema_changes() %}

    {% set schema_changes_query -%}

    {# column_removed #}
    alter table {{ ref('groups') }}
        drop column group_a;

    {# column_removed #}
    alter table {{ ref('stats_players') }}
        drop column offsides;

    {# column_added #}
    alter table {{ ref('stats_players') }}
        add red_cards varchar(100);

    {# column_added #}
    alter table {{ ref('stats_players') }}
        add key_crosses varchar(100);

    {# column_type_change #}
    alter table {{ ref('groups') }}
        drop column group_b;
    alter table {{ ref('groups') }}
        add group_b integer;

    {# table_removed #}
    drop table {{ ref('stats_team') }};

    {%- endset %}

    {% do run_query(schema_changes_query) %}

{% endmacro %}

