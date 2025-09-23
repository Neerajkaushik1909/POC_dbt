{{config(materialized='ephemeral')}}
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
        r_num=1)
        Select c.*,cld.max_publish_ts as csr_latest_update_date 
        FROM compute_company_data c 
        LEFT JOIN 
        {{ ref('csr_latest_date') }} cld 
        ON cast(c.company_id as string) = cld.company_id 