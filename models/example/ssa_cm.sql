select
    c.company_id as csr_company_id,
    case
        when c.is_reseller = true
        then null
        when cp.account_id is not null
        then cp.account_id
        when cs.account_id is not null
        then cs.account_id
        else null
    end as sfdc_account_id,
    case
        when c.is_reseller = true
        then null
        when cp.contract_id is not null
        then cp.contract_id
        when cs.contract_id is not null
        then cs.contract_id
        else null
    end as contract_id,
    case
        when c.is_reseller = true
        then null
        when cast(cp.contract_number as string) is not null
        then cp.contract_number
        when cast(cs.contract_number as string) is not null
        then cast(cs.contract_number as string)
        else null
    end as contract_number,
    case
        when c.is_reseller = true
        then null
        when cp.account_name is not null
        then cp.account_name
        when cs.account_name is not null
        then cs.account_name
        else null
    end as account_name,
    case
        when coc.company_id is not null
        then coc.start_date
        else
            case
                when c.is_reseller = true
                then
                    case
                        when c.plan_period = 'yearly'
                        then
                            case
                                when
                                    date_sub(c.next_billing_date, interval 1 year)
                                    <= current_date()
                                then date_sub(c.next_billing_date, interval 1 year)
                                else
                                    date_sub(
                                        date_sub(c.next_billing_date, interval 1 year),
                                        interval 14 day
                                    )
                            end
                        when c.plan_period = 'monthly'
                        then
                            case
                                when
                                    date_sub(c.next_billing_date, interval 1 month)
                                    <= current_date()
                                then date_sub(c.next_billing_date, interval 1 month)
                                else
                                    date_sub(
                                        date_sub(c.next_billing_date, interval 1 month),
                                        interval 14 day
                                    )
                            end
                        when c.plan_period = 'quarterly'
                        then
                            case
                                when
                                    date_sub(c.next_billing_date, interval 3 month)
                                    <= current_date()
                                then date_sub(c.next_billing_date, interval 3 month)
                                else
                                    date_sub(
                                        date_sub(c.next_billing_date, interval 3 month),
                                        interval 14 day
                                    )
                            end
                        when c.plan_period = 'semiannual'
                        then
                            case
                                when
                                    date_sub(c.next_billing_date, interval 6 month)
                                    <= current_date()
                                then date_sub(c.next_billing_date, interval 6 month)
                                else
                                    date_sub(
                                        date_sub(c.next_billing_date, interval 6 month),
                                        interval 14 day
                                    )
                            end
                    end
                when cp.contract_start_date is not null
                then cp.contract_start_date
                when cs.contract_start_date is not null
                then cs.contract_start_date
                when c.under_contract = false
                then
                    case
                        when c.plan_period = 'yearly'
                        then
                            case
                                when
                                    date_sub(c.next_billing_date, interval 1 year)
                                    <= current_date()
                                then date_sub(c.next_billing_date, interval 1 year)
                                else
                                    date_sub(
                                        date_sub(c.next_billing_date, interval 1 year),
                                        interval 14 day
                                    )
                            end
                        when c.plan_period = 'monthly'
                        then
                            case
                                when
                                    date_sub(c.next_billing_date, interval 1 month)
                                    <= current_date()
                                then date_sub(c.next_billing_date, interval 1 month)
                                else
                                    date_sub(
                                        date_sub(c.next_billing_date, interval 1 month),
                                        interval 14 day
                                    )
                            end
                        when c.plan_period = 'quarterly'
                        then
                            case
                                when
                                    date_sub(c.next_billing_date, interval 3 month)
                                    <= current_date()
                                then date_sub(c.next_billing_date, interval 3 month)
                                else
                                    date_sub(
                                        date_sub(c.next_billing_date, interval 3 month),
                                        interval 14 day
                                    )
                            end
                        when c.plan_period = 'semiannual'
                        then
                            case
                                when
                                    date_sub(c.next_billing_date, interval 6 month)
                                    <= current_date()
                                then date_sub(c.next_billing_date, interval 6 month)
                                else
                                    date_sub(
                                        date_sub(c.next_billing_date, interval 6 month),
                                        interval 14 day
                                    )
                            end
                    end
                else null
            end
    end as start_date,
    case
        when coc.company_id is not null
        then coc.end_date
        else
            case
                when c.is_reseller = true
                then null
                when cp.contract_end_date is not null
                then cp.contract_end_date
                when cs.contract_end_date is not null
                then cs.contract_end_date
                when c.under_contract = false
                then null
                else null
            end
    end as end_date,
    'Activated' as contract_status,
    case
        when coc.company_id is not null and coc.end_date is null
        then true
        when coc.company_id is not null and coc.end_date is not null
        then false
        when c.is_reseller = true
        then true
        when cp.contract_start_date is not null
        then false
        when cs.contract_start_date is not null
        then false
        when c.under_contract = false
        then true
        else cast(null as boolean)
    end as is_evergreen_contract,
    case
        when coc.company_id is not null
        then 'override_customer'
        when c.is_reseller = true
        then 'csr'
        when cp.contract_id is not null
        then 'sfdc_pubsub'
        when cs.contract_id is not null
        then 'sfdc'
        when c.under_contract = false
        then 'csr'
        else null
    end as source_system,
    current_timestamp as date_created,
    current_timestamp as date_modified,
    cast(csr_latest_update_date as timestamp) as csr_latest_update_date,
    case
        when cs.sfdc_last_updated_date is not null
        then cast(cp.sfdc_last_updated_date as timestamp)
        when cp.sfdc_last_updated_date is not null
        then cast(cs.sfdc_last_updated_date as timestamp)
        else null
    end as sfdc_last_updated_date
from {{ ref("company_data") }} c
left join
    (
        select *
        from {{ source("raw_netsuite", "contract_override_customers_table") }}
        where is_override = true
    ) coc
    on c.company_id = cast(coc.company_id as string)
left join {{ ref("contracts_pubsub") }} cp on c.company_id = cp.dialpad_company_id
left join
    {{ ref("contracts_sfdc") }} cs  -- tested working fine.
    on c.company_id = cs.company_id
where not (cp.contract_id is null and coalesce(cs.totalrows, 1) > 1)
