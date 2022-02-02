{% macro strings_list_to_tuple(args) %}
    {%- if args|length > 0 -%}
        ({%- for arg in args -%} '{{ arg }}' {{ "," if not loop.last else "" }} {%- endfor -%})
    {%- else -%}
        ('')
    {%- endif -%}
{% endmacro %}


{% macro lower_strings_list_to_tuple(args) %}
    {%- if args|length > 0 -%}
        ({%- for arg in args -%}
             {%- set lower_arg = arg|lower -%}
            '{{- lower_arg -}}'
            {{- "," if not loop.last else "" -}}
        {%- endfor -%})
    {%- else -%}
        ('')
    {%- endif -%}
{% endmacro %}


{% macro like_strings_list_to_tuple(args, left_string='', right_string='') %}
    {%- if args|length > 0 -%}
        ({%- for arg in args -%}
             {%- set lower_arg = arg|lower -%}
                {{ "'" ~ '%' ~ left_string ~ lower_arg ~ right_string ~ '%' ~ "'" }}
            {{- " ," if not loop.last else "" -}}
        {%- endfor -%})
    {%- else -%}
        ('')
    {%- endif -%}
{% endmacro %}