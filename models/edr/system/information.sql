{{
  config(
    materialized = 'incremental',
    bind=False
  )
}}

{{ elementary.empty_information_table() }}
