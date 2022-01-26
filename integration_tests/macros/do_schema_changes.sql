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

    {# column_type_change_should_not_alert #}
    alter table {{ ref('matches') }}
    drop column home;
    alter table {{ ref('matches') }}
    add home integer;

    {# table_removed #}
    drop table {{ ref('stats_team') }};

    {# table_added #}
    create table {{ target.database ~"."~ target.schema }}_data.stadiums (
    stadium_name varchar,
    location varchar,
    capacity integer
    );

    {# table_added_should_not_alert #}
    create table {{ target.database ~"."~ target.schema }}_data.scores (
    new_col varchar
    );

    {%- endset %}

    {% do run_query(schema_changes_query) %}

{% endmacro %}

