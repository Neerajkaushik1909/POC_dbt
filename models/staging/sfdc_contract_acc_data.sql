{{ config(materialized='ephemeral') }}

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
            {{ contract_acc_join_conditions() }}