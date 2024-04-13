
{{ config(materialized='table') }}


select 
    date_trunc('week', sod.order_date) as order_week
    , sod.status
    , count(sod.order_pk) as count
from 
    {{ref('hub_order')}} ho 
    left join {{ref('sat_order_details')}} sod on ho.order_pk=sod.order_pk
{{ dbt_utils.group_by(n=2) }}
