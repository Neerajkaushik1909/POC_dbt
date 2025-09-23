-- models/company_max_publish.sql
with
    updates as (
        select cast(company.id as string) as company_id, publish_timestamp
        from
            {{ source("raw", "csr_plan_updates") }} as billing,
            unnest(billing.plan_update.update_fields) as update_field
    )
select distinct
    up.company_id,
    max(up.publish_timestamp) over (
        partition by up.company_id order by up.publish_timestamp desc
    ) as max_publish_ts
from updates up
left join {{ ref('second') }} sm on up.company_id = sm.company_id
