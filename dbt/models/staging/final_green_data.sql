{{ config(materialized='table') }}

SELECT *
FROM {{ref('green_month')}}