{{
  config(
    materialized = 'table',
    bind=False,
    post_hook='{{ elementary.upload_information() }}'
  )
}}

{{ elementary.empty_information_table() }}
