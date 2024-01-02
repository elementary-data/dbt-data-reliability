{% macro get_rendered_ref(ref_string) %}
  {% set lowered_ref_string = ref_string | lower %}
  {% set match = modules.re.match("(ref\('(?P<ref_identifier>.+)'\))", lowered_ref_string, modules.re.IGNORECASE) %}
  {% if not match %}
    {% do return(lowered_ref_string) %}
  {% else %}
    {% do return(ref(match.groupdict()['ref_identifier'])['include']()) %}
  {% endif %}
{% endmacro %}
