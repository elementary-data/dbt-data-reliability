{%- macro edr_concat(val1, val2) -%}
    concat({{ elementary.edr_cast_as_string(val1) }}, {{ elementary.edr_cast_as_string(val2) }})
{%- endmacro -%}

{% macro edr_list_concat(values, separator="") %}
    {% set result %}
        concat(
            {%- for value in values -%}
                {{- value -}}
            {%- if not loop.last -%}
                ,
                {%- if separator -%}
                    {{- "'" ~ separator ~ "'" -}}
                    ,
                {%- endif -%}
            {%- endif -%}
            {%- endfor -%}
        )
    {% endset %}
    {{ log(result, info=True) }}
    {{ return(result) }}
{% endmacro %}