{{ config(materialized='ephemeral') }}

        SELECT
            *
            FROM
            {{source("raw","salesforce_contracts")}}
            WHERE
            dialpad_company_id IS NOT NULL
            AND contract.contract_id IS NOT NULL
            AND CAST(contract.contract_start_date AS date)<=CURRENT_DATE()
            AND CAST(contract.contract_end_date AS date)>=CURRENT_DATE()