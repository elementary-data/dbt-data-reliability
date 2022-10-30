{{
  config(
    materialized='table',
    transient=False,
    post_hook='{{ elementary.upload_information() }}'
  )
}}

{{ elementary.empty_information_table() }}
