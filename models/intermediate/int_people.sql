with stg_people_mazuria as (

	select * from {{ref('stg_people_mazuria')}}

),

final as (

	select 
		customer_id,
		date_add,
		customer_name,
		customer_phone,
		customer_status,
		row_number() over(partition by customer_phone order by date_add) as rn
	from stg_people_mazuria
	where customer_id is not null

),

first_customer_status as (

	select
		customer_phone,
		customer_status as first_customer_status,
		date_add as first_date_add
	from final
	where rn = 1

),

second_customer_status as (

	select
		customer_phone,
		customer_status as second_customer_status,
		date_add as second_date_add
	from final
	where rn = 2

),

prefinal as (

	select 
		distinct customer_id,
		final.customer_phone,
		customer_name,
		case when second_customer_status is null then first_customer_status else  second_customer_status end as current_status,
		first_date_add,
		second_date_add,
		first_customer_status,
		second_customer_status
	from final
	left join first_customer_status on first_customer_status.customer_phone = final.customer_phone
	left join second_customer_status on second_customer_status.customer_phone = final.customer_phone
	order by customer_name

)

select * from prefinal

