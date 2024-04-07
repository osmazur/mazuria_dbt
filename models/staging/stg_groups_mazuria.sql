
with groups_mazuria_raw as (

	select 
		*
	from {{ source('raw', 'groups_mazuria_raw') }}

),

final as (

	select 
-- group info
		group_name,
		date(group_end) as course_end_date,
        date(group_start) as course_start_date,
-- course info
		course_lentgh_month,
        course_name,
		course_price,
-- other
        payment_num,
        is_active, -- calc from the spreasheet
		case 
		    when date(group_start) <= date(current_date) and group_end is null then 'active'
		    when group_start is not null and group_end is not null then 'ended' 
		    when (group_start is null or date(group_start) < date(current_date)) and group_end is null then 'not_started_yet' 
		else null 
        end as course_status
	from groups_mazuria_raw

)

select * from final
