
with mazuria_gs_raw_groups as (

	select 
		*
	from {{ source('raw', 'mazuria_gs_raw_groups') }}

),

final as (

	select 
		id,
        name as group_name,
        course_name,
        start_date as group_start_date,
        end_date as group_end_date,
        lentgh_fact as group_lentgh_fact,
        is_active
	from mazuria_gs_raw_groups

)

select * from final
