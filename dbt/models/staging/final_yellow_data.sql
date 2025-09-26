{{ config(materialized='table') }}

SELECT *
FROM {{ref('yellow_month')}}