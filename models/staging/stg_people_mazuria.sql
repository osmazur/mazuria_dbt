
with people_mazuria_raw as (

	select *
	from {{ source('raw', 'people_mazuria_raw') }}
),

final as (

	select 
		id as customer_id,
		date as date_add,
		group_name,
-- customers info
        customer_name,
		course_name,
		customer_status,
        		customer_phone,
-- payments
		first_payment_date,
		second_payment_date,
		third_payment_date,
-- other
		comment
	from people_mazuria_raw
)

select * from final
