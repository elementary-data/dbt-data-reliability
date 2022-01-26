{% macro do_configuration_changes() %}

    {% set configuration_changes_query -%}

    {# schema_to_false #}
    update {{ ref('monitoring_configuration') }}
    set alert_on_schema_changes = false
    where lower(schema_name) = lower('{{ target.schema ~ "_data" }}')
          and table_name is null and column_name is null
    ;

    {# table_added_should_not_alert #}
    create table {{ target.database ~"."~ target.schema }}_data.new_table (
    new_col varchar
    );

    {# table_removed_should_not_alert #}
    drop table {{ target.database ~"."~ target.schema }}_data.stadiums;

    {# table_to_false #}
    update {{ ref('monitoring_configuration') }}
    set alert_on_schema_changes = false
    where lower(table_name) = 'matches'
        and column_name is null
    ;

    {# column_type_change_should_not_alert #}
    alter table {{ ref('matches') }}
    drop column round_group;
    alter table {{ ref('matches') }}
    add round_group integer;

    {# table_to_false #}
    update {{ ref('monitoring_configuration') }}
    set alert_on_schema_changes = false
    where lower(table_name) = lower('groups')
        and column_name is null
    ;

    {# table_removed_should_not_alert #}
    drop table {{ ref('groups') }};

    {# column_to_false #}
    update {{ ref('monitoring_configuration') }}
    set alert_on_schema_changes = false
    where lower(column_name) = lower('player')
    ;

    {# column_removed_should_not_alert #}
    alter table {{ ref('stats_players') }}
    drop column player;

    {# column_add_should_alert #}
    alter table {{ ref('stats_players') }}
    add age integer;

    {# column_to_true #}
    update {{ ref('monitoring_configuration') }}
    set alert_on_schema_changes = true
    where lower(column_name) = 'home'
    ;

    {# column_type_change_should_alert #}
    alter table {{ ref('matches') }}
    drop column home;
    alter table {{ ref('matches') }}
    add home boolean;

    {%- endset %}

    {% do run_query(configuration_changes_query) %}

{% endmacro %}

