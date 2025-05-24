{{ config(
    materialized='table'
) }}
select * from {{ source('raw', 'order_details') }}