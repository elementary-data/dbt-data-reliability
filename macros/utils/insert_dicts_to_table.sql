{% macro insert_dicts_to_table(table_name, list_of_dicts) %}
    insert into {{ table_name }} {{- strings_list_to_tuple(list_of_dicts[0].keys()) }}
           values
        {% for dict in list_of_dicts -%}
            {{ strings_list_to_tuple(dict.values()) }} {{ "," if not loop.last else "" }}
        {% endfor -%}
{% endmacro %}
