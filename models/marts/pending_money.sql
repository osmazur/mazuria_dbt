

with prep as (

	select
		gr.group_name,
		customer_name,
		customer_phone,
		first_payment_date,
		second_payment_date,
		third_payment_date,
		course_price,
			case when  
				customer_status = 'is_customer'  
				and second_payment_date is null 
				and first_payment_date is not null 
				then (first_payment_date::date + interval '30' day)::date
			end as future_second_payment,
			case when  
					customer_status = 'is_customer'  
					and third_payment_date is null  
					and second_payment_date is not null 
					then (second_payment_date::date + interval '30' day)::date 
				when 
					customer_status = 'is_customer'  
					and first_payment_date is not null 
					and second_payment_date is  null 
					and third_payment_date is null
				then (first_payment_date::date + interval '60' day)::date
			end as future_third_payment,
	   count  (distinct case when  customer_status = 'is_customer'  and first_payment_date is null  then customer_phone end ) as num_first_payment_not,
	   count  (distinct case when  customer_status = 'is_customer'  and second_payment_date is null then customer_phone end ) as num_second_payment_not,
	   count  (distinct case when  customer_status = 'is_customer'  and third_payment_date is null then customer_phone end ) as num_third_payment_not
	   --case when  customer_status = 'is_customer'  and second_payment_date is null and first_payment_date is not null then first_payment_date end as first_payment_date,
	   --case when  customer_status = 'is_customer'  and third_payment_date is null  and second_payment_date is not null then second_payment_date  end as second_payment_date
	from {{ref('stg_people_mazuria')}} as pip
	left join {{ref('stg_groups_mazuria')}} as gr on gr.group_name = pip.group_name
	where course_status = 'active'
	group by 1,2,3,4,5,6,7,customer_status, second_payment_date, third_payment_date, first_payment_date
),

finale as (

	select 
		*, 
		coalesce(future_second_payment,future_third_payment) as not_null
	from prep
)

select 
	*,
	course_price * num_second_payment_not as pending_money_second,
	course_price * num_third_payment_not as pending_money_third
from finale where not_null is not null