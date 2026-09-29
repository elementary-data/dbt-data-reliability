{% macro format_owner_email(email) %}
    {#
        dbt allows owner.email to be a list (dbt-core 1.10+). Store the owner
        emails as a single ";"-joined string so the owner_email column stays a
        plain string.
    #}
    {% set emails = elementary.normalize_owners(email) %}
    {% do return(emails | join(";") if emails else none) %}
{% endmacro %}
