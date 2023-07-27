{% macro delete_if_incremental(where_clause) %}

    {% set query%}
        delete from {{ this }}
        where {{ where_clause }}
    {% endset %}

    {% if is_incremental() %}
        {% do elementary.run_query(query) %}
    {% endif %}

{% endmacro %}