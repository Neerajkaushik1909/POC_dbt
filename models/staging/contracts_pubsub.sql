{{config(materialized='ephemeral')}}

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
        FROM  {{ref('sfdc_contract_prov')}} ) con
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
        AND COALESCE(s_con.status, 'Activated')='Activated'