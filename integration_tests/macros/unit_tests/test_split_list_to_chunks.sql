{% macro test_split_list_to_chunks() %}
    {% set result = elementary.split_list_to_chunks(item_list=[1,2,3],
                                                    chunk_size=1) %}
    {{ assert_value(result, [[1], [2], [3]]) }}

    {% set result = elementary.split_list_to_chunks(item_list=[{'bla': [{'bla': [1]},2,3]},
                                                               {'yay': [{'dada': [17]},2,3,5,6]},
                                                               {'bla': [{'bla': [1]},2,3]}],
                                                    chunk_size=2) %}
    {{ assert_value(result, [[{'bla': [{'bla': [1]},2,3]}, {'yay': [{'dada': [17]},2,3,5,6]}],
                             [{'bla': [{'bla': [1]},2,3]}]]) }}

    {% set result = elementary.split_list_to_chunks(item_list=[1,2,3,4],
                                                    chunk_size=2) %}
    {{ assert_value(result, [[1,2], [3,4]]) }}

    {% set result = elementary.split_list_to_chunks(item_list=[1],
                                                    chunk_size=2) %}
    {{ assert_value(result, [[1]]) }}

    {% set result = elementary.split_list_to_chunks(item_list=[1,2,3,4,5],
                                                    chunk_size=2) %}
    {{ assert_value(result, [[1,2], [3,4], [5]]) }}

    {% set result = elementary.split_list_to_chunks(item_list=[1,2,3,4,5,6],
                                                    chunk_size=2) %}
    {{ assert_value(result, [[1,2], [3,4], [5,6]]) }}
{% endmacro %}
