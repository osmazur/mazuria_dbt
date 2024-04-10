
with mazuria_gs_raw_courses as (

	select 
		*
	from {{ source('raw', 'mazuria_gs_raw_courses') }}

),

final as (

	select 
        id,
        course_email_id,
        name as course_name,
        name_latin as course_name_latin,
        type as course_type,
        lentgh as course_lentgh_plan,
        payment_num as payment_number,
        price as course_price,
        second_payment_on,
        valid_from,
        valid_to
	from mazuria_gs_raw_courses

)

select * from final
