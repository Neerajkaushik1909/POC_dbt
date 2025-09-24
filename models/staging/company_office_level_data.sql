{{ config(materialized='ephemeral') }}
SELECT
          company.id AS company_id,
          office.plan.reseller.id AS reseller_id,
          CAST(PARSE_DATETIME('%FT%H:%M:%E*SZ', NULLIF(office.plan.next_billing_date, '')) AS DATE) AS next_billing_date,
          office.plan.plan_period AS plan_period,
          company.under_contract,
          company.is_accounting_exempt,
          office.state AS office_state,
          office.plan.state AS plan_state,
          office.plan.payment_method AS payment_method,
          -- MAX(publish_timestamp) OVER(PARTITION BY company.id ORDER BY publish_timestamp desc) as max_publish_timestamp,
          ROW_NUMBER()OVER(PARTITION BY company.id ORDER BY publish_timestamp DESC) AS row_num
        FROM
          {{ unnest_csr_plan_update() }}

          Qualify row_num =1