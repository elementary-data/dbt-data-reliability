{% macro assert_dicts_are_equal(dict1, dict2) -%}
    {% set diff_keys = [] -%}

    {# Compare the keys in dict1 with dict2 #}
    {% for key in dict1.keys() -%}
        {% if key not in dict2.keys()  -%}
            {% do diff_keys.append(key) -%}
        {%- endif %}
    {%- endfor %}

    {# Compare the keys in dict2 with dict1 #}
    {% for key in dict2.keys() -%}
        {% if key not in dict1.keys() -%}
            {% do diff_keys.append(key) -%}
        {%- endif %}
    {%- endfor %}

    {# Compare the values for the common keys in both dictionaries #}
    {% set diff_values = [] -%}
    {% for key, value in dict1 -%}
        {% if dict1[key] != dict2[key] -%}
            {% do diff_values.append(key ~': '~ value) -%}
        {%- endif %}
    {%- endfor %}

    {# Assert if there are any differences #}
    {% if diff_keys or diff_values -%}
        {%- set msg = 'The dictionaries are not equal. Differences found:' -%}
        {% for key in diff_keys -%}
            {% set msg = msg ~ ' ' ~ key ~ ',' -%}
        {%- endfor %}
        {% for key in diff_values -%}
            {% set msg = msg ~ ' ' ~ key ~ ':' + dict1[key] ~ '!=' + dict2[key] ~ ',' -%}
        {%- endfor %}
        {% set msg = msg[:-1] ~ '.' %}
        {% do print(msg) %}
    {%- endif %}
{%- endmacro %}