with fct_sf_sql_func as (

	select 
		*
	from {{ref('fct_sf_sql_func')}}

)

select * from fct_sf_sql_func