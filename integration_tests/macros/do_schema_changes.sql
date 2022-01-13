{% macro do_schema_changes() %}

    {% set schema_changes_query -%}
    alter table {{ target.database ~"."~ target.schema }}_data.GROUPS
    drop column GROUP_A;

    alter table {{ target.database ~"."~ target.schema }}_data.STATS_PLAYERS
    add RED_CARDS varchar(100);

    alter table {{ target.database ~"."~ target.schema }}_data.STATS_PLAYERS
    add KEY_CROSSES varchar(100);

    alter table {{ target.database ~"."~ target.schema }}_data.STATS_PLAYERS
    drop column OFFSIDES;

    alter table {{ target.database ~"."~ target.schema }}_data.GROUPS
    drop column GROUP_B;

    alter table {{ target.database ~"."~ target.schema }}_data.GROUPS
    add GROUP_B integer;

    drop table {{ target.database ~"."~ target.schema }}_data.STATS_TEAM;

    create table {{ target.database ~"."~ target.schema }}_data.stadiums (
    stadium_name varchar,
    location varchar,
    capacity integer
    );

    {%- endset %}

    {% do run_query(schema_changes_query) %}

{% endmacro %}

