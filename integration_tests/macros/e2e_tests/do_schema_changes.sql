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
        add column red_cards {{ elementary.type_string() }};

    {# column_added #}
    alter table {{ ref('stats_players') }}
        add column key_crosses {{ elementary.type_string() }};

    {# column_type_change #}
    alter table {{ ref('stats_team') }}
        drop column goals;
    alter table {{ ref('stats_team') }}
        add column goals {{ dbt_utils.type_string() }};



    {%- endset %}
    {% do run_query(schema_changes_query) %}
    {% do elementary.edr_log("schema changes executed successfully") %}

{% endmacro %}

