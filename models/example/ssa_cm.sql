WITH
    compute_company_data as (
      SELECT
      CAST(rc_row.company_id AS string) AS company_id,
      CASE
        WHEN sub_ref.sub_reseller_id IS NOT NULL THEN CAST(sub_ref.reseller_id AS string)
        WHEN ref.reseller_id IS NOT NULL THEN CAST(ref.reseller_id AS string)
        ELSE CAST(rc_row.company_id AS string)
        END
        AS reseller_id,
        CASE
            WHEN sub_ref.sub_reseller_id IS NOT NULL THEN TRUE
            WHEN ref.reseller_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END
        AS is_reseller,
        next_billing_date,
        plan_period,
        under_contract,
        -- csr_date.max_publish_ts as csr_latest_update_date,
        ROW_NUMBER() OVER(PARTITION BY rc_row.company_id, COALESCE(sub_ref.reseller_id,ref.reseller_id,rc_row.company_id)) AS r_num
        FROM (
        SELECT
            company_id,
            reseller_id,
            next_billing_date,
            plan_period,
            under_contract,
            -- DATE(max_publish_timestamp) as csr_latest_update_date
      FROM {{ ref('company_office_level_data') }} 
      WHERE
        {{suite_billing_criteria()}})rc_row
        LEFT JOIN
        {{source('raw_netsuite','referenced_resellers')}} ref
        ON
        CAST(rc_row.reseller_id AS string) = CAST(ref.reseller_id AS string)
        LEFT JOIN (
        SELECT
            id AS sub_reseller_id,
            rollup_id AS reseller_id
        FROM
            {{source("raw_reference_rollup","reference_rollup")}}
        WHERE
            rollup_type='reseller') sub_ref
        ON
        CAST(rc_row.reseller_id AS string) = CAST(sub_ref.sub_reseller_id AS string)
        QUALIFY
        r_num=1),

    company_data as(
        Select c.*,cld.max_publish_ts as csr_latest_update_date 
        FROM compute_company_data c 
        LEFT JOIN 
        {{ ref('csr_latest_date') }} cld 
        ON cast(c.company_id as string) = cld.company_id ),
    contracts_pubsub AS (
        SELECT
        con.* EXCEPT(rownumber),
        acc.name AS account_name
        FROM (
        SELECT
            dialpad_company_id,
            contract.contract_id,
            contract.contract_number,
            account_id,
            CAST(contract.contract_start_date AS date) AS contract_start_date,
            CAST(contract.contract_end_date AS date) AS contract_end_date,
            ROW_NUMBER() OVER (PARTITION BY dialpad_company_id ORDER BY source_metadata.event_timestamp DESC) AS rownumber,
            MAX(PARSE_TIMESTAMP('%Y%m%d %H%M%S', REGEXP_REPLACE(time, r'\.\d+$', ''))) OVER(PARTITION BY dialpad_company_id ORDER BY time desc) AS sfdc_last_updated_date
        FROM (
            SELECT
            *
            FROM
            {{source("raw","salesforce_contracts")}}
            WHERE
            dialpad_company_id IS NOT NULL
            AND contract.contract_id IS NOT NULL
            AND CAST(contract.contract_start_date AS date)<=CURRENT_DATE()
            AND CAST(contract.contract_end_date AS date)>=CURRENT_DATE())) con
        LEFT JOIN
        {{source("raw_uat","account")}} acc
        ON
        con.account_id=acc.id
        LEFT JOIN
        {{source("raw_uat","contract")}} s_con
        ON
        con.contract_id=CAST(s_con.id as STRING)
        AND con.contract_number=CAST(s_con.contract_number as string)
        WHERE
        con.rownumber=1
        AND COALESCE(s_con.status, 'Activated')='Activated'),
    contracts_sfdc AS (
        SELECT
        a.*,
        CASE
            WHEN totalrows>1 THEN FALSE
            ELSE TRUE
        END
        isValid
        FROM (
        SELECT
            * REPLACE((
            SELECT
                STRING_AGG(DISTINCT i)
            FROM
                c.contract_id i ) AS contract_id )
        FROM (
            SELECT
            acc.uv_company_id_c AS company_id,
            acc.name AS account_name,
            con.contract_number,
            con.account_id,
            CAST(con.end_date AS date) AS contract_end_date,
            CAST(con.start_date AS date) AS contract_start_date,
            MAX((Last_CSR_Sync_DateTime__c)) OVER(PARTITION BY con.id order by Last_CSR_Sync_DateTime__c desc) as sfdc_last_updated_date,
            ROW_NUMBER() OVER(PARTITION BY con.account_id ORDER BY con.end_date) AS row_num,
            ARRAY_AGG(con.id) OVER (PARTITION BY con.account_id) AS contract_id,
            COUNT(DISTINCT con.id) OVER (PARTITION BY con.account_id) AS totalrows
            FROM
            {{source("raw_uat","contract")}} con,
            {{source("raw_uat","account")}} acc
            WHERE
            acc.id=con.account_id
            AND con.status='Activated'
            AND con.is_deleted=FALSE
            AND CAST(start_date AS date)<=CURRENT_DATE()
            AND CAST(end_date AS date)>=CURRENT_DATE()) c) a
        WHERE
        row_num = 1)
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
  company_data c
LEFT JOIN (
  SELECT
    *
  FROM
  {{source("raw_netsuite","contract_override_customers_table")}}
  WHERE
    Is_override=TRUE) coc
ON
  c.company_id=CAST(coc.company_id AS string)
LEFT JOIN
  contracts_pubsub cp
ON
  c.company_id=cp.dialpad_company_id
LEFT JOIN
  contracts_sfdc cs
ON
  c.company_id=cs.company_id
WHERE
  NOT(cp.contract_id IS NULL AND COALESCE(cs.totalrows,1)>1) 