{% macro do_schema_changes() %}
    {{- return(adapter.dispatch('do_schema_changes')()) -}}
{% endmacro %}

{% macro default__do_schema_changes() %}

    {% set schema_changes_query -%}

    drop table {{ ref('groups') }};
    drop table {{ ref('stats_players') }};
    drop table {{ ref('stats_team') }};
    alter table {{ ref('groups_validation') }} rename to {{ ref('groups').identifier }};
    alter table {{ ref('stats_players_validation') }} rename to {{ ref('stats_players').identifier }};
    alter table {{ ref('stats_team_validation') }} rename to {{ ref('stats_team').identifier }};

    {%- endset %}
    {% do run_query(schema_changes_query) %}
    {% do elementary.edr_log("schema changes executed successfully") %}

{% endmacro %}

{% macro bigquery__do_schema_changes() %}

    {% set schema_changes_query -%}

    drop table {{ ref('groups') }};
    drop table {{ ref('stats_players') }};
    drop table {{ ref('stats_team') }};
    alter table {{ ref('groups_validation') }} rename to {{ elementary.quote_column(ref('groups').identifier) }};
    alter table {{ ref('stats_players_validation') }} rename to {{ elementary.quote_column(ref('stats_players').identifier) }};
    alter table {{ ref('stats_team_validation') }} rename to {{ elementary.quote_column(ref('stats_team').identifier) }};

    {%- endset %}
    {% do run_query(schema_changes_query) %}
    {% do elementary.edr_log("schema changes executed successfully") %}

{% endmacro %}