    with base as (

    select
        acc.uv_company_id_c as company_id,
        acc.name as account_name,
        con.contract_number,
        con.account_id,
        cast(con.end_date as date) as contract_end_date,
        cast(con.start_date as date) as contract_start_date,

        max(date(Last_CSR_Sync_DateTime__c)) over (
            partition by con.id 
            order by Last_CSR_Sync_DateTime__c desc
        ) as sfdc_last_updated_date,

        row_number() over (
            partition by con.account_id 
            order by con.end_date
        ) as row_num,

        array_agg(con.id) over (
            partition by con.account_id
        ) as contract_id,

        count(distinct con.id) over (
            partition by con.account_id
        ) as totalrows

    from {{ source('raw_uat', 'contract') }} con
    join {{ source('raw_uat', 'account') }} acc
      on acc.id = con.account_id

    where con.status = 'Activated'
      and con.is_deleted = false
      and cast(start_date as date) <= current_date()
      and cast(end_date as date) >= current_date()

),

deduped as (

    select
        * replace (
            (
                select string_agg(distinct i)
                from unnest(c.contract_id) i
            ) as contract_id
        )
    from base c

),

final as (

    select
        a.*,
        case when totalrows > 1 then false else true end as isValid
    from deduped a
    where row_num = 1

)
select * from final