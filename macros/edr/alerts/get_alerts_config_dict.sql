-- Keep backcomp
-- Get alerts_config dict from config, if not found from meta
-- Merge config from model and test level
-- For subscribers union lists, for the rest the lower granularity config overrides
-- docs - changed channel to alert_channel

{% macro get_alerts_config_dict(node_dict) %}
    {%- if node_dict.get('resource_type') == 'test' %}
        {%- set test_node_dict = node_dict %}
        {%- set parent_model_unique_id = elementary.get_primary_test_model_unique_id_from_test_node(test_node_dict) %}
        {%- set model_node_dict = elementary.get_node(parent_model_unique_id) %}
    {%- endif %}

    {%- set alerts_config_backcomp = elementary.get_alerts_config_backcomp(model_node_dict, test_node_dict) %}
    {%- if alerts_config_backcomp %}
        {{ return(alerts_config_backcomp) }}
    {%- endif %}

    {%- set alert_suppression_interval = elementary.get_config_argument(argument_name='alert_suppression_interval', value=none, model_graph_node=model_node_dict, test_graph_node=test_node_dict) %}
    {%- set alert_channel = elementary.get_config_argument(argument_name='alert_channel', value=none, model_graph_node=model_node_dict, test_graph_node=test_node_dict) %}
    {%- set alert_fields = elementary.get_config_argument(argument_name='alert_fields', value=none, model_graph_node=model_node_dict, test_graph_node=test_node_dict) %}
    {%- set slack_group_alerts_by = elementary.get_config_argument(argument_name='slack_group_alerts_by', value=none, model_graph_node=model_node_dict, test_graph_node=test_node_dict) %}
    {%- set subscribers = elementary.get_alert_subscribers(model_node_dict, test_node_dict) %}

    {% set alerts_config =
      {'alert_suppression_interval': alert_suppression_interval,
       'channel': alert_channel,
       'alert_fields': alert_fields,
       'slack_group_alerts_by': slack_group_alerts_by,
       'subscribers': subscribers
        } %}
    {%- set test_configuration = elementary.empty_dict_keys_to_none(test_configuration) -%}
    {{ return(alerts_config) }}
{% endmacro %}



{% macro get_alert_subscribers(model_node_dict, test_node_dict) %}
    {%- set model_subscribers = elementary.get_config_argument('subscribers', value=none, model_graph_node=model_node_dict, test_graph_node=none) %}
    {%- set test_subscribers = elementary.get_config_argument('subscribers', value=none, model_graph_node=none, test_graph_node=test_node_dict) %}
    {%- set subscribers = elementary.union_lists(model_subscribers, test_subscribers) %}
    {{ return(subscribers) }}
{% endmacro %}

{% macro get_alerts_config_backcomp(model_node_dict, test_node_dict) %}
    {%- set model_alerts_config = elementary.get_config_argument('alerts_config', value=none, model_graph_node=model_node_dict, test_graph_node=none) %}
    {%- set test_alerts_config = elementary.get_config_argument('alerts_config', value=none, model_graph_node=none, test_graph_node=test_node_dict) %}
    {%- if model_alerts_config or test_alerts_config %}
        {%- set alerts_config = {} %}
        {%- do alerts_config.update(model_alerts_config) -%}
        {%- do alerts_config.update(test_alerts_config) -%}
        {# We want to union subscribers, not override #}
        {%- set model_subscribers = elementary.safe_get_with_default(model_alerts_config, 'subscribers', []) %}
        {%- set test_subscribers = elementary.safe_get_with_default(test_alerts_config, 'subscribers', []) %}
        {%- set subscribers = elementary.union_lists(model_subscribers, test_subscribers) %}
        {%- do alerts_config.update({'subscribers': subscribers}) -%}
        {{ return(alerts_config) }}
    {%- endif %}
    {{ return(none) }}
{% endmacro %}