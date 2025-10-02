{% test exposure_schema_validity(model, exposures, node, columns) %}
    {{ config(tags = ['elementary-tests']) }}
    {%- if not execute or not elementary.is_test_command() or not elementary.is_elementary_enabled() -%}
        {%- do return(none) -%}
    {%- endif -%}

    {%- if dbt_version <= '1.3.0' -%}
        {# attached_node is only available on newer dbt versions #}
        {%- set base_node = context['model']['depends_on']['nodes'][0] -%}
    {%- else -%}
        {%- set base_node = context['model']['attached_node'] -%}
    {%- endif -%}

    {# Parameters used only for dependency injection in integration tests #}
    {%- set node = node or base_node -%}
    {%- set exposures = (exposures or graph.exposures).values() -%}
    {%- set columns = columns or adapter.get_columns_in_relation(model)  -%}

    {%- set model_relation = elementary.get_model_relation_for_test(model, elementary.get_test_model()) -%}
    {%- set full_table_name = elementary.relation_to_full_name(model_relation) -%}
    {{- elementary.test_log('start', full_table_name, 'exposure validation') -}}

    {%- set matching_exposures = [] -%}

    {%- for exposure in exposures -%}
        {#

        We need the 'meta' property to be defined in the exposure, since column level info is not available on exposures.
        The 'meta' property needs to have a 'referenced_columns' array (see properties in the next comment)

        #}
        {%- if node in exposure.depends_on.nodes and (exposure['meta'] or none) is not none -%}
            {%- do matching_exposures.append(exposure) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if matching_exposures | length > 0 -%}
        {%- set columns_dict = {} -%}
        {%- for column in columns -%}
            {%- do columns_dict.update({ column['name'].strip('"').strip("'") | upper : elementary.normalize_data_type(elementary.get_column_data_type(column)) }) -%}
        {%- endfor -%}
        {%- set invalid_exposures = [] -%}
        {%- for exposure in matching_exposures -%}
            {%- set meta = exposure['meta'] or none -%}
            {%- if meta != none and (meta['referenced_columns'] or none) is iterable -%}
                {%- for exposure_column in meta['referenced_columns'] -%}
                    {#
                        Each column in 'referenced_columns' has the following property:

                        'column_name'

                        Optionally, you can specify the following properties:

                        'data_type' - the specific data type of the column

                        'node' - the ref to the source node of the specific column used in the exposure

                    #}
                    {%- if matching_exposures | length > 1 and 'node' not in exposure_column -%}
                        {%- do elementary.edr_log_warning("missing node property for the exposure: " ~ exposure['name'] ~ " which not the only exposure depending on " ~ model ~ ", We're not able to verify the column level dependencies of this exposure") -%}
                    {%- elif matching_exposures | length == 1 or (context['render'](elementary.get_rendered_ref((exposure_column['node'] or ''))) == context['render'](model | lower)) -%}
                        {%- if exposure_column['column_name'] | upper not in columns_dict.keys() -%}
                            {%- do invalid_exposures.append({
                                    'exposure': exposure['name'],
                                    'url': exposure['url'],
                                    'error': exposure_column['column_name'] ~ ' column missing in the model'
                                    })
                            -%}
                        {%- elif (exposure_column['data_type'] or '') != '' and exposure_column['data_type'] != columns_dict[exposure_column['column_name'] | upper] -%}
                            {%- do invalid_exposures.append({
                                    'exposure': exposure['name'],
                                    'url': exposure['url'],
                                    'error': 'different data type for the column ' ~ exposure_column['column_name'] ~ ' ' ~ exposure_column['data_type'] ~ ' vs ' ~ columns_dict[exposure_column['column_name'] | upper]
                                    })
                            -%}
                        {%- endif -%}
                    {%- endif -%}
                {%- endfor -%}
            {%- else -%}
                {%- do elementary.edr_log_warning("missing meta property for the exposure: " ~ exposure['name'] ~ ", We're not able to verify the column level dependencies of this exposure") -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if invalid_exposures | length > 0 -%}
            {%- for invalid_exposure in invalid_exposures %}
                {{ 'UNION ALL ' if not loop.first }}SELECT '{{ invalid_exposure['exposure'] }}' as exposure, '{{ invalid_exposure['url'] }}' as url, '{{ invalid_exposure['error'] }}' as error
            {%- endfor -%}
        {%- else -%}
            {{ elementary.no_results_query() }}
        {%- endif -%}
    {%- else -%}
    {{ elementary.no_results_query() }}

    {%- endif -%}
    {{ elementary.test_log('end', full_table_name, 'exposure validation') }}
{% endtest %}
