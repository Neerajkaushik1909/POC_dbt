
{{ config(materialized='ephemeral') }}
select distinct company_id, max_publish_ts
        from (
            select
                max(publish_timestamp) over (
                    partition by company.id order by publish_timestamp desc
                ) as max_publish_ts,
                cast(company.id as string) as company_id
            from {{ unnest_csr_plan_update() }}
            where {{ apply_ssa_filters() }}
        )