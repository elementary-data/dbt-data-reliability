{% macro edr_commit() %}
    {% do return(adapter.dispatch("edr_commit", "elementary")()) %}
{% endmacro %}

{% macro default__edr_commit() %} {% do adapter.commit() %} {% endmacro %}

{# Callers pass should_commit when their writes must be durable; this checks
   whether there is a transaction to commit. On dbt-sqlserver 1.12 both happen:
   Elementary's queries run with auto_begin=false and usually autocommit, leaving
   nothing open (adapter.commit() would raise), but an earlier auto_begin statement
   such as truncate_relation opens a real transaction that must be committed or it
   is rolled back. commit_if_open exists from dbt-sqlserver 1.11; older versions
   always have one open here. #}
{% macro sqlserver__edr_commit() %}
    {% if adapter.commit_if_open is defined %} {% do adapter.commit_if_open() %}
    {% else %} {% do adapter.commit() %}
    {% endif %}
{% endmacro %}
