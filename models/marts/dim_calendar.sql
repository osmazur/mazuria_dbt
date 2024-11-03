with int_finance_card as (

	select 
		*
	from {{ref('int_calendar')}}

)

select * from calendar