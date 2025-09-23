
{% macro contract_acc_join_conditions() %}
            acc.id=con.account_id
            AND con.status='Activated'
            AND con.is_deleted=FALSE
            AND CAST(start_date AS date)<=CURRENT_DATE()
            AND CAST(end_date AS date)>=CURRENT_DATE()

{% endmacro %}