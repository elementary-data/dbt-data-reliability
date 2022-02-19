{% macro insert_dicts_to_table(table_name, dict_list) -%}
    {%- set timestamp_type -%}
        {{- dbt_utils.type_timestamp() -}}
    {%- endset -%}
    {% set insert_dicts_query %}
        insert into {{ table_name }}
            {% set columns = adapter.get_columns_in_relation(table_name) -%}
            ({%- for column in columns -%}
                {{- column.name -}} {{- "," if not loop.last else "" -}}
            {%- endfor -%}) values
            {% for dict in dict_list -%}
                ({%- for column in columns -%}
                    {%- if column.is_string() or column.dtype.lower() == timestamp_type.strip().lower() -%}
                        {%- if column.name in dict -%}
                            '{{dict[column.name]}}'
                        {%- elif column.name.lower() in dict -%}
                            '{{dict[column.name.lower()]}}'
                        {%- else -%}
                            NULL
                        {%- endif -%}
                    {%- elif column.is_number() -%}
                        {%- if column.name in dict -%}
                            {{-dict[column.name]-}}
                        {%- elif column.name.lower() in dict -%}
                            {{-dict[column.name.lower()]-}}
                        {%- else -%}
                            NULL
                        {%- endif -%}
                    {%- else -%}
                        NULL
                    {%- endif -%}
                    {{- "," if not loop.last else "" -}}
                {%- endfor -%}) {{- "," if not loop.last else "" -}}
            {%- endfor -%}
    {% endset %}
    {% do run_query(insert_dicts_query) %}
{%- endmacro %}
