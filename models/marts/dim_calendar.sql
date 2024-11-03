with calendar as (

	select 
		*
	from {{ref('int_calendar')}}

)

select * from calendar