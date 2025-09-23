{% macro apply_ssa_filters() %}
    plan_products.product_id = updated_products.product_id
    and plan_pricing_schema.license_type = case
        when updated_pricing_schema.license_type = 'default'
        then 'contract'
        else updated_pricing_schema.license_type
    end
    and coalesce(plan_products.parent_product_id, 'NA')
        = coalesce(updated_products.parent_product_id, 'NA')
    and (
        (
            (source_metadata.event_source = 'customer'
             or source_metadata.event_source is null)
            and plan_update.update_type = 'user'
        )
        or (
            source_metadata.event_source = 'csr'
            and source_metadata.category = 'on behalf of customer'
        )
    )
    and update_field in ('product_units', 'product_price')
    and plan_products.product_id not in (
        'e911 Service', 'Compliance and Administrative Cost Recovery Fee'
    )
    and updated_products.product_id not in (
        'e911 Service', 'Compliance and Administrative Cost Recovery Fee'
    )
    and (
        updated_products.total_units_delta is not null
        or updated_products.total_units_delta != 0
    )
    and company.under_contract = true
    and office.state = 'active'
    and office.plan.state = 'paid'
    and office.plan.payment_method in ('invoiced', 'credit_card')
{% endmacro %}
