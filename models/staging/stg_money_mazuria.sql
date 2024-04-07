
with money_mazuria_raw as (

	select 
		*
	from {{ source('raw', 'money_mazuria_raw') }}

),

final as (

	select 
		task as task_name,
		name as customer_name,
        teacher as teacher_name,
		task_type,
		group_name,
		total_price,
		date as date_add,
		month::date as month_add,
		costs_type,
		transaction_type,
		payment_type,
		textile_type,
		textile_size,
		price_bought,
		price_sell,
		comment
	from money_mazuria_raw

)

select * from final
