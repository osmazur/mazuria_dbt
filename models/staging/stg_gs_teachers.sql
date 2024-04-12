
with mazuria_gs_raw_teachers as (

	select 
		*
	from {{ source('raw', 'mazuria_gs_raw_teachers') }}

),

final as (

	select 
		first_name as teacher_first_name,
        last_name as teacher_last_name,
        full_name as teacher_full_name,
        phone as teacher_phone,
        start_date as teacher_start_date,
        end_date as teacher_end_date,
        birthday,
        employment_type,
        mazuria_email as teacher_mazuria_email,
        valid_from,
        valid_to
	from mazuria_gs_raw_teachers

)

select * from final
