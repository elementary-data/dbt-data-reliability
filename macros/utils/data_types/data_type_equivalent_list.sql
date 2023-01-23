{% macro exact_data_type_equivalent_list(exact_data_type) %}
    {% set result = adapter.dispatch('exact_data_type_equivalent_list','elementary')(exact_data_type) %}
    {{ return(result) }}
{% endmacro %}

{% macro default__exact_data_type_equivalent_list(exact_data_type) %}

 {% set exact_data_type_to_data_type_returned_by_the_info_schema = {'VARCHAR': 'TEXT',
                'STRING': 'TEXT'}%}
 {%- if exact_data_type in exact_data_type_to_data_type_returned_by_the_info_schema%}
   {{ return (exact_data_type_to_data_type_returned_by_the_info_schema[exact_data_type])}}
 {%- else %}
   {{return (exact_data_type) }}
 {%- endif%}
{% endmacro %}

{% macro bigquery__exact_data_type_equivalent_list(exact_data_type) %}

 {% set exact_data_type_to_data_type_returned_by_the_info_schema = {'VARCHAR': 'TEXT',
                'STRING': 'TEXT'}%}
 {%- if exact_data_type in exact_data_type_to_data_type_returned_by_the_info_schema%}
   {{ return (exact_data_type_to_data_type_returned_by_the_info_schema[exact_data_type])}}
 {%- else %}
   {{return (exact_data_type) }}
 {%- endif%}
{% endmacro %}



{% macro snowflake__exact_data_type_equivalent_list(exact_data_type) %}

 {% set exact_data_type_to_data_type_returned_by_the_info_schema = {'VARCHAR': 'TEXT',
                'STRING': 'TEXT'}%}
 {%- if exact_data_type in exact_data_type_to_data_type_returned_by_the_info_schema%}
   {{ return (exact_data_type_to_data_type_returned_by_the_info_schema[exact_data_type])}}
 {%- else %}
   {{return (exact_data_type) }}
 {%- endif%}
{% endmacro %}



{% macro spark__exact_data_type_equivalent_list(exact_data_type) %}

 {% set exact_data_type_to_data_type_returned_by_the_info_schema = {'VARCHAR': 'TEXT',
                'STRING': 'TEXT'}%}
 {%- if exact_data_type in exact_data_type_to_data_type_returned_by_the_info_schema%}
   {{ return (exact_data_type_to_data_type_returned_by_the_info_schema[exact_data_type])}}
 {%- else %}
   {{return (exact_data_type) }}
 {%- endif%}
{% endmacro %}
