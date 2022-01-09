{% macro union_all_diff_dbs(monitored_dbs_list, query_macro_name) %}
    {% for db in monitored_dbs_list %}
        {{ query_macro_name(db) }}
        {% if not loop.last %}
        union all
        {% endif %}
    {% endfor %}
{% endmacro %}


{% macro union_all_diff_dbs_filtered(monitored_dbs_list, query_macro_name, filter_clause) %}
    {% for db in monitored_dbs_list %}
        {{ query_macro_name(db) }}
        {{ filter_clause }}
        {% if not loop.last %}
        union all
        {% endif %}
    {% endfor %}
{% endmacro %}