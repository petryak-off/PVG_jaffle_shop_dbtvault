with actual_customer as (
    select
    hc.customer_pk as customer_pk,
    scd.EFFECTIVE_FROM as scd_effective_from,
    scdc.EFFECTIVE_FROM as scdc_effective_from,
    coalesce(lead(scd.EFFECTIVE_FROM) over (partition by hc.customer_pk order by scd.EFFECTIVE_FROM), '9999-12-31') as scd_effective_to,
    coalesce(lead(scdc.EFFECTIVE_FROM) over (partition by hc.customer_PK order by scdc.EFFECTIVE_FROM), '9999-12-31') as scdc_effective_to
    
    from {{ref('hub_customer')}} hc
    left join {{ref('sat_customer_details')}} scd on hc.CUSTOMER_PK=scd.CUSTOMER_PK
    left join {{ref('sat_customer_details_crm')}} scdc on hc.CUSTOMER_PK=scdc.CUSTOMER_PK
)

select 
customer_pk
, scd_effective_from
, scdc_effective_from
, scd_effective_to
, scdc_effective_to
from actual_customer