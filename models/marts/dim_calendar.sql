with calendar as (

	select 
		*
	from {{ref('stg_calendar')}}

)

select * from calendar