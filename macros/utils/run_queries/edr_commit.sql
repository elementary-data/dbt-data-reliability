{% macro edr_commit() %}
    {% do return(adapter.dispatch("edr_commit", "elementary")()) %}
{% endmacro %}

{% macro default__edr_commit() %} {% do adapter.commit() %} {% endmacro %}

{# dbt-sqlserver 1.12 no longer opens a transaction on metadata reads such as
   get_columns_in_relation, and Elementary's own queries run with auto_begin=false,
   so there may be no open transaction and adapter.commit() raises.
   commit_if_open exists from dbt-sqlserver 1.11; older versions always have one open. #}
{% macro sqlserver__edr_commit() %}
    {% if adapter.commit_if_open is defined %} {% do adapter.commit_if_open() %}
    {% else %} {% do adapter.commit() %}
    {% endif %}
{% endmacro %}
