with mazuria_gs_raw__google_calendar as (

	select 
		*
	from "mazuria"."mazuria_raw"."mazuria_gs_raw__google_calendar"

),

final as (

	select 
		event_id,
        event_title,
-- date and time
        start_date as event_start_date,
        start_time as event_start_time,
        end_date as event_end_date,
        end_time as event_end_time,

        guests as event_guests,
        is_finished,
        color as event_color,
        description as event_description,
        loaded_at,
        is_removed,
        Null as course_email_id
	from mazuria_gs_raw__google_calendar

)

select * from final