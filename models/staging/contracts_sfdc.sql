
{{ config(materialized='ephemeral') }}

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
            SELECT STRING_AGG(DISTINCT i) FROM c.contract_id i ) AS contract_id )
        FROM  {{ref("sfdc_contract_acc_data")}}  c) a
        WHERE
        row_num = 1