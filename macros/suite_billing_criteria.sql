{% macro suite_billing_criteria() %}


COALESCE(is_accounting_exempt,FALSE)=FALSE
        AND plan_state IN ('paid',
          'hold')
        AND office_state='active'
        AND payment_method in ('invoiced', ' credit_card')


        {% endmacro %}