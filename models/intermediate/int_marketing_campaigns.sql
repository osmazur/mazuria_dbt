{{
  config(
    enabled = false
  )
}}
-- test

with money_mazuria_raw as (

	select 
		*
	from {{ref('stg_people_mazuria')}}

),

final as (

		select 
		date_add,
		customer_phone,
		customer_status,
		customer_name,
		campaing_source,
		marketing_campaing_name,
		marketing_camp_started,
		marketing_camp_ended,
		comment
	from money_mazuria_raw
	where marketing_campaing_name is not null

)

select * from final
