
with mazuria_gs_raw__customers_history as (

	select *
	from {{ source('raw', 'mazuria_gs_raw__customers_history') }}
),

final as (

	select 
		id as customer_history_id,
        created_date as customer_created_date,
--customer info
        name as customer_name,
        phone as customer_phone,
        status as customer_status,
        comment as customer_comment,
-- other
        group_name,
        course_name,
--payments
        first_payment_date,
        second_payment_date,
        third_payment_date
	from mazuria_gs_raw__customers_history
)

select * from final
