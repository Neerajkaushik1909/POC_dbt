
SELECT
  c.company_id AS csr_company_id,
  CASE
    WHEN c.is_reseller=TRUE THEN NULL
    WHEN cp.account_id IS NOT NULL THEN cp.account_id
    WHEN cs.account_id IS NOT NULL THEN cs.account_id
    ELSE NULL
END AS sfdc_account_id,
  CASE
    WHEN c.is_reseller=TRUE THEN NULL
    WHEN cp.contract_id IS NOT NULL THEN cp.contract_id
    WHEN cs.contract_id IS NOT NULL THEN cs.contract_id
    ELSE NULL
END
  AS contract_id,
  CASE
    WHEN c.is_reseller=TRUE THEN NULL
    WHEN CAST(cp.contract_number as STRING) IS NOT NULL THEN cp.contract_number
    WHEN CAST(cs.contract_number as STRING) IS NOT NULL THEN CAST(cs.contract_number as STRING)
    ELSE NULL
END
  AS contract_number,
  CASE
    WHEN c.is_reseller=TRUE THEN NULL
    WHEN cp.account_name IS NOT NULL THEN cp.account_name
    WHEN cs.account_name IS NOT NULL THEN cs.account_name
    ELSE NULL
END
  AS account_name,
  CASE
    WHEN coc.company_id IS NOT NULL THEN coc.start_date
    ELSE CASE
    WHEN c.is_reseller=TRUE THEN CASE
    WHEN c.plan_period='yearly' THEN
  CASE
    WHEN DATE_SUB(c.next_billing_date, INTERVAL 1 year)<= CURRENT_DATE() THEN DATE_SUB(c.next_billing_date, INTERVAL 1 year)
    ELSE DATE_SUB(DATE_SUB(c.next_billing_date, INTERVAL 1 year), INTERVAL 14 day)
END
    WHEN c.plan_period='monthly' THEN CASE
    WHEN DATE_SUB(c.next_billing_date, INTERVAL 1 month)<= CURRENT_DATE() THEN DATE_SUB(c.next_billing_date, INTERVAL 1 month)
    ELSE DATE_SUB(DATE_SUB(c.next_billing_date, INTERVAL 1 month), INTERVAL 14 day)
END
    WHEN c.plan_period='quarterly' THEN CASE
    WHEN DATE_SUB(c.next_billing_date, INTERVAL 3 month)<= CURRENT_DATE() THEN DATE_SUB(c.next_billing_date, INTERVAL 3 month)
    ELSE DATE_SUB(DATE_SUB(c.next_billing_date, INTERVAL 3 month), INTERVAL 14 day)
END
    WHEN c.plan_period='semiannual' THEN CASE
    WHEN DATE_SUB(c.next_billing_date, INTERVAL 6 month)<= CURRENT_DATE() THEN DATE_SUB(c.next_billing_date, INTERVAL 6 month)
    ELSE DATE_SUB(DATE_SUB(c.next_billing_date, INTERVAL 6 month), INTERVAL 14 day)
END
END
    WHEN cp.contract_start_date IS NOT NULL THEN cp.contract_start_date
    WHEN cs.contract_start_date IS NOT NULL THEN cs.contract_start_date
    WHEN c.under_contract =FALSE THEN CASE
    WHEN c.plan_period='yearly' THEN
  CASE
    WHEN DATE_SUB(c.next_billing_date, INTERVAL 1 year)<= CURRENT_DATE() THEN DATE_SUB(c.next_billing_date, INTERVAL 1 year)
    ELSE DATE_SUB(DATE_SUB(c.next_billing_date, INTERVAL 1 year), INTERVAL 14 day)
END
    WHEN c.plan_period='monthly' THEN CASE
    WHEN DATE_SUB(c.next_billing_date, INTERVAL 1 month)<= CURRENT_DATE() THEN DATE_SUB(c.next_billing_date, INTERVAL 1 month)
    ELSE DATE_SUB(DATE_SUB(c.next_billing_date, INTERVAL 1 month), INTERVAL 14 day)
END
    WHEN c.plan_period='quarterly' THEN CASE
    WHEN DATE_SUB(c.next_billing_date, INTERVAL 3 month)<= CURRENT_DATE() THEN DATE_SUB(c.next_billing_date, INTERVAL 3 month)
    ELSE DATE_SUB(DATE_SUB(c.next_billing_date, INTERVAL 3 month), INTERVAL 14 day)
END
    WHEN c.plan_period='semiannual' THEN CASE
    WHEN DATE_SUB(c.next_billing_date, INTERVAL 6 month)<= CURRENT_DATE() THEN DATE_SUB(c.next_billing_date, INTERVAL 6 month)
    ELSE DATE_SUB(DATE_SUB(c.next_billing_date, INTERVAL 6 month), INTERVAL 14 day)
END
END
    ELSE NULL
END
END
  AS start_date,
  CASE
    WHEN coc.company_id IS NOT NULL THEN coc.end_date
    ELSE CASE
    WHEN c.is_reseller=TRUE THEN NULL
    WHEN cp.contract_end_date IS NOT NULL THEN cp.contract_end_date
    WHEN cs.contract_end_date IS NOT NULL THEN cs.contract_end_date
    WHEN c.under_contract =FALSE THEN NULL
    ELSE NULL
END
END
  AS end_date,
  'Activated' AS contract_status,
  CASE
    WHEN coc.company_id IS NOT NULL AND coc.end_date IS NULL THEN TRUE
    WHEN coc.company_id IS NOT NULL
  AND coc.end_date IS NOT NULL THEN FALSE
    WHEN c.is_reseller=TRUE THEN TRUE
    WHEN cp.contract_start_date IS NOT NULL THEN FALSE
    WHEN cs.contract_start_date IS NOT NULL THEN FALSE
    WHEN c.under_contract =FALSE THEN TRUE
    ELSE CAST(NULL AS boolean)
END
  AS is_evergreen_contract,
  CASE
    WHEN coc.company_id IS NOT NULL THEN 'override_customer'
    WHEN c.is_reseller=TRUE THEN 'csr'
    WHEN cp.contract_id IS NOT NULL THEN 'sfdc_pubsub'
    WHEN cs.contract_id IS NOT NULL THEN 'sfdc'
    WHEN c.under_contract =FALSE THEN 'csr'
    ELSE NULL
END
  AS source_system,
  current_timestamp AS date_created,
  current_timestamp AS date_modified,
  CAST(csr_latest_update_date as timestamp) as csr_latest_update_date,
  CASE
    WHEN cs.sfdc_last_updated_date IS NOT NULL THEN CAST(cp.sfdc_last_updated_date as timestamp)
    WHEN cp.sfdc_last_updated_date IS NOT NULL THEN CAST(cs.sfdc_last_updated_date as timestamp)
    ELSE NULL
END
  AS sfdc_last_updated_date
FROM
  {{ref("company_data")}} c
LEFT JOIN (SELECT * FROM {{source("raw_netsuite","contract_override_customers_table")}} WHERE Is_override=TRUE) coc
ON
  c.company_id=CAST(coc.company_id AS string)
LEFT JOIN
  {{ref("contracts_pubsub")}} cp
ON
  c.company_id=cp.dialpad_company_id
LEFT JOIN
  {{ref("contracts_sfdc")}} cs --tested working fine.
ON
  c.company_id=cs.company_id
WHERE
  NOT(cp.contract_id IS NULL AND COALESCE(cs.totalrows,1)>1) 