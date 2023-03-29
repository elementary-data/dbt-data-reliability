{% macro edr_day_of_week_mod_7(date_expr) %}
    {# for same days of the week, itll always return the same int. However, not the same int between runs. #}
    {# so e.g. Monday and Monday-last-week will both be 0 this run , and if I run tomorrow they both will be 1.#}
    {% set anchor_date = elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_max_bucket_end())) %}
    {% set ret %}
    {{ elementary.edr_datediff(first_date=elementary.edr_cast_as_timestamp(date_expr),
                               second_date=anchor_date,
                               date_part='day') }} % 7
    {% endset %}
    {%do return(ret) %}
{% endmacro %}