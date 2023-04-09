{% macro construct_metric_properties_dict(timestamp_column,
                                          where_expression,
                                          time_bucket,
                                          freshness_column=none,
                                          event_timestamp_column=none,
                                          seasonality=none,
                                          days_back=none) %}
  {# each one of the params will go into the returned dictionary, unless "Undefined" which would be converted to "None" #}
  {%set d = {'timestamp_column': ( timestamp_column if timestamp_column else None ) ,
             'where_expression': ( where_expression if where_expression else None ) ,
             'time_bucket':      ( time_bucket if time_bucket else None )           ,
             'freshness_column': (freshness_column if freshness_column else None  ) ,
             'event_timestamp_column':(event_timestamp_column if event_timestamp_column
                                                              else None           ) ,
             'seasonality':      (seasonality if seasonality else None            ) ,
             'days_back':        (days_back if days_back else None                )} %}
  {% do return(d) %}
{% endmacro %}