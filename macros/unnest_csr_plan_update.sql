{% macro unnest_csr_plan_update() %}
    {{ source("raw", "csr_plan_updates") }} billing,
    unnest(billing.plan_update.update_fields) as update_field,
    unnest(billing.plan_update.products) as updated_products,
    unnest(billing.office.plan.products) as plan_products
    left join unnest(updated_products.pricing_schema) as updated_pricing_schema
    left join unnest(plan_products.pricing_schema) as plan_pricing_schema
{% endmacro %}
