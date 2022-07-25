{% macro do_schema_changes() %}

    {% set schema_changes_query -%}

    {# column_removed #}
    {# Dropping the column group_a from the table groups. #}
    alter table {{ ref('groups') }}
        drop column group_a;

    {# column_removed #}
    {# Dropping the column offsides from the stats_players table. #}
    alter table {{ ref('stats_players') }}
        drop column offsides;

    {# column_added #}
    {# Adding a column called red_cards to the stats_players table. #}
    alter table {{ ref('stats_players') }}
        add column red_cards {{ elementary.type_string() }};

    {# column_added #}
    {# Adding a column called key_crosses to the stats_players table. #}
    alter table {{ ref('stats_players') }}
        add column key_crosses {{ elementary.type_string() }};

    {# column_type_change #}
    {# Dropping the column goals from the stats_team table and then adding it back in with a different type. #}
    alter table {{ ref('stats_team') }}
        drop column goals;
    alter table {{ ref('stats_team') }}
        add column goals {{ dbt_utils.type_string() }};



    {%- endset %}
    {% do run_query(schema_changes_query) %}
    {% do elementary.edr_log("schema changes executed successfully") %}

{% endmacro %}

