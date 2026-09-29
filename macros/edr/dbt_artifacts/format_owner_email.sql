{% macro format_owner_email(email) %}
    {#
        dbt allows owner.email to be a list (dbt-core 1.10+). Store a list as a
        single ";"-joined string so the owner_email column stays a plain string.
    #}
    {% if email is string or email is none %} {% do return(email) %} {% endif %}
    {% if email is iterable and email is not mapping %}
        {% set emails = [] %}
        {% for item in email %}
            {% if item is string and item | trim %}
                {% do emails.append(item | trim) %}
            {% endif %}
        {% endfor %}
        {% do return(emails | join(";") if emails else none) %}
    {% endif %}
    {% do return(email) %}
{% endmacro %}
