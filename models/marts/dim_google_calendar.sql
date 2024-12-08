with int_google_calendar as (

	select 
		*
	from {{ref('int_google_calendar')}}

)

select * from int_google_calendar